import torch
import torch.nn as nn
import torch.nn.functional as F

# ==========================================
# 1. 构建微缩版大模型 (用于模拟 Actor/Ref/Critic/RM)
# ==========================================
class MiniLLM(nn.Module):
    def __init__(self, vocab_size=100, embed_dim=32):
        super().__init__()
        self.vocab_size = vocab_size
        self.emb = nn.Embedding(vocab_size, embed_dim)
        # 用 GRU 模拟 Transformer 处理序列
        self.rnn = nn.GRU(embed_dim, embed_dim, batch_first=True)
        # 输出每个词的概率 (Language Model Head)
        self.lm_head = nn.Linear(embed_dim, vocab_size)
        # 输出当前状态的价值 (Value Head, 供 Critic 和 RM 使用)
        self.value_head = nn.Linear(embed_dim, 1)

    def forward(self, x):
        """输入 shape: [batch, seq_len]"""
        embeds = self.emb(x)
        h, _ = self.rnn(embeds)
        logits = self.lm_head(h)                  # shape: [batch, seq_len, vocab_size]
        values = self.value_head(h).squeeze(-1)   # shape: [batch, seq_len]
        return logits, values

    @torch.no_grad()
    def generate(self, prompt, max_new_tokens=10):
        """模拟自回归生成"""
        curr = prompt
        for _ in range(max_new_tokens):
            logits, _ = self(curr)
            # 取最后一个时间步的输出，计算概率并采样下一个词
            probs = F.softmax(logits[:, -1, :], dim=-1)
            next_token = torch.multinomial(probs, num_samples=1)
            # 拼接到当前序列上
            curr = torch.cat([curr, next_token], dim=1)
        return curr

# ==========================================
# 2. 完整的 PPO 训练器
# ==========================================
class PPOTrainer:
    def __init__(self, actor, ref_model, critic, reward_model):
        self.actor = actor
        self.ref_model = ref_model
        self.critic = critic
        self.reward_model = reward_model
        
        self.kl_coef = 0.02      # KL 散度惩罚系数
        self.clip_ratio = 0.2    # PPO 截断比例 ε
        self.gamma = 0.99        # 折扣因子
        self.lam = 0.95          # GAE 参数
        
        # 优化器
        self.actor_optim = torch.optim.Adam(self.actor.parameters(), lr=1e-3)
        self.critic_optim = torch.optim.Adam(self.critic.parameters(), lr=5e-3)

    def _get_logprobs(self, logits, tokens):
        """
        [核心难点 1] 提取模型在实际生成的 token 上的对数概率
        logits: [batch, seq_len, vocab_size]
        tokens: [batch, seq_len]
        """
        # 计算所有的对数概率分布
        log_probs = F.log_softmax(logits, dim=-1)
        # 使用 gather 函数，把模型实际选中的那个 token 的概率“抠”出来
        # gather 后的 shape 是 [batch, seq_len, 1]，最后去掉最后一维
        actual_logprobs = torch.gather(log_probs, dim=-1, index=tokens.unsqueeze(-1)).squeeze(-1)
        return actual_logprobs

    def _compute_advantages(self, rewards, values):
        """
        [核心难点 2] 使用广义优势估计 (GAE) 逆向计算每个 token 的 Advantage
        """
        batch_size, seq_len = rewards.shape
        advantages = torch.zeros_like(rewards)
        last_gae_lam = 0
        
        # 从最后生成的 token 开始往前回溯计算
        for t in reversed(range(seq_len)):
            if t == seq_len - 1:
                next_values = 0.0 # 序列末尾没有未来价值
            else:
                next_values = values[:, t + 1]
            
            # TD Error (时序差分误差): 当前奖励 + 未来的期望 - 当前的估值
            delta = rewards[:, t] + self.gamma * next_values - values[:, t]
            
            # 累加优势
            advantages[:, t] = last_gae_lam = delta + self.gamma * self.lam * last_gae_lam
            
        # 计算 Returns (用于更新 Critic)
        returns = advantages + values
        return advantages.detach(), returns.detach()

    def train_step(self, prompts):
        # ================= 阶段 1：与环境交互收集数据 (Rollout) =================
        with torch.no_grad():
            # 1. 生成完整序列 (Prompt + Response)
            inputs = self.actor.generate(prompts, max_new_tokens=10)
            
            # 2. 获取旧模型的概率与价值预测
            actor_logits, _ = self.actor(inputs)
            ref_logits, _ = self.ref_model(inputs)
            _, values = self.critic(inputs)
            
            old_logprobs = self._get_logprobs(actor_logits, inputs)
            ref_logprobs = self._get_logprobs(ref_logits, inputs)
            
            # 3. 计算 KL 惩罚 (防止模型胡言乱语)
            kl_penalty = old_logprobs - ref_logprobs
            
            # 4. 获取奖励模型的评分 (模拟人类偏好，这里取 Value Head 的最终输出)
            _, rm_values = self.reward_model(inputs)
            rm_scores = rm_values[:, -1] # 只取最后一个 token 的得分作为期末奖励
            
            # 5. 分配 Token 级别的奖励
            rewards = -self.kl_coef * kl_penalty
            # 把期末大奖加在最后一个 token 上
            rewards[:, -1] += rm_scores

        # ================= 阶段 2：PPO 网络更新 =================
        # 计算优势函数和目标回报 (这里不再使用 no_grad，因为不需要算前向梯度)
        advantages, returns = self._compute_advantages(rewards, values)
        
        # 标准化 Advantage，让训练更稳定
        advantages = (advantages - advantages.mean()) / (advantages.std() + 1e-8)

        # 重新做一次前向传播，获取带梯度的计算图
        current_logits, current_values = self.actor(inputs)
        current_logprobs = self._get_logprobs(current_logits, inputs)
        _, current_values_from_critic = self.critic(inputs)
        
        # 1. 计算 Actor Loss
        ratio = torch.exp(current_logprobs - old_logprobs)
        surr1 = ratio * advantages
        surr2 = torch.clamp(ratio, 1.0 - self.clip_ratio, 1.0 + self.clip_ratio) * advantages
        actor_loss = -torch.min(surr1, surr2).mean()

        # 2. 计算 Critic Loss
        critic_loss = F.mse_loss(current_values_from_critic, returns)

        # 3. 梯度反向传播
        self.actor_optim.zero_grad()
        actor_loss.backward()
        self.actor_optim.step()

        self.critic_optim.zero_grad()
        critic_loss.backward()
        self.critic_optim.step()

        return actor_loss.item(), critic_loss.item(), rm_scores.mean().item()

# ==========================================
# 3. 运行测试主循环
# ==========================================
if __name__ == "__main__":
    print("初始化 4 个模型...")
    # 模拟初始化 4 个模型 (实际情况中应该使用预训练好的模型加载权重)
    actor = MiniLLM()
    ref_model = MiniLLM()
    critic = MiniLLM()
    reward_model = MiniLLM()

    # 冻结 Ref 和 Reward 模型的权重
    ref_model.eval()
    reward_model.eval()

    # 将 Ref 模型的初始权重完全同步给 Actor
    actor.load_state_dict(ref_model.state_dict())

    trainer = PPOTrainer(actor, ref_model, critic, reward_model)

    print("\n开始 PPO 训练循环...")
    # 构造一个固定的 Dummy Prompt (假设 batch size=4, 长度为2的输入)
    # 例如：[用户]: 你好
    dummy_prompts = torch.randint(0, 100, (4, 2))

    for epoch in range(1, 51):
        # 每次用这个 prompt 触发 PPO 训练
        actor_loss, critic_loss, mean_reward = trainer.train_step(dummy_prompts)
        
        if epoch % 5 == 0:
            print(f"迭代 {epoch:2d} | "
                  f"Actor Loss: {actor_loss: .4f} | "
                  f"Critic Loss: {critic_loss: .4f} | "
                  f"平均奖励 (Reward): {mean_reward: .4f}")

    print("\n训练完成！你会看到 '平均奖励' 稳步上升，说明 Actor 学会了如何讨好 Reward Model。")