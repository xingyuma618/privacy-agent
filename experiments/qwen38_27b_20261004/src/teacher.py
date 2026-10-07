from __future__ import annotations
import json
import threading
import time
from openai import OpenAI
import httpx
from common import stable_hash

class Teacher:
    def __init__(self, config, case_key):
        self.config = config
        self.case_key = case_key
        self.calls = []
        self.clients = [OpenAI(base_url=url, api_key='EMPTY', timeout=config.get('timeout_seconds', 180), max_retries=2, http_client=httpx.Client(trust_env=False)) for url in config['endpoints']]
        self.index = int(stable_hash(case_key)[:8], 16) % len(self.clients)

    def call(self, role, messages, tools=None, temperature=0, n=1):
        started = time.monotonic()
        seed = (self.config['seed'] + int(stable_hash([self.case_key, role, len(self.calls)])[:8], 16)) % (2**31)
        kwargs = dict(model=self.config['teacher_alias'], messages=messages, temperature=temperature, max_tokens=self.config['max_tokens'][role], n=n, seed=seed, extra_body={'chat_template_kwargs': {'enable_thinking': False}})
        if tools is not None:
            kwargs.update(tools=tools, tool_choice='auto', parallel_tool_calls=False)
        try:
            response = self.clients[self.index].chat.completions.create(**kwargs)
        except Exception as e:
            self.calls.append({'role': role, 'seconds': time.monotonic()-started, 'error': str(e), 'seed': seed})
            raise
        choices = []
        for choice in response.choices:
            if choice.finish_reason == 'length':
                # A truncated response is not a valid terminal action or label.
                choices.append({'role': 'assistant', 'content': '', '_truncated': True})
                continue
            message = {'role': 'assistant', 'content': choice.message.content or ''}
            if choice.message.tool_calls:
                message['tool_calls'] = [tc.model_dump(exclude_none=True) for tc in choice.message.tool_calls]
            choices.append(message)
        self.calls.append({'role': role, 'seconds': time.monotonic()-started, 'seed': seed, 'n': n, 'prompt_tokens': response.usage.prompt_tokens if response.usage else None, 'completion_tokens': response.usage.completion_tokens if response.usage else None, 'returned_model': response.model, 'finish_reasons': [c.finish_reason for c in response.choices]})
        return choices

    def json(self, role, prompt):
        message = self.call(role, [{'role':'system','content':'Return only valid JSON. Do not use markdown.'},{'role':'user','content':prompt}])[0]
        if message.get('_truncated'):
            raise ValueError(f'{role} JSON output was truncated.')
        text = message['content'].strip()
        if text.startswith('```'):
            text = text.split('\n', 1)[1].rsplit('```', 1)[0]
        # Strict JSON: malformed scores and labels cannot silently pass.
        return json.loads(text)
