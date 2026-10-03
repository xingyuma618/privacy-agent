# Third-party sources

The release retains the project-local snapshots and modifications of:

- LLaMA-Factory: `LlamaFactory/`, upstream https://github.com/hiyouga/LLaMA-Factory ; original LICENSE retained.
- verl: `privacy-agent/verl/`, upstream https://github.com/volcengine/verl ; original LICENSE and NOTICE retained where present.
- CHESS: `privacy-agent/CHESS/`, upstream https://github.com/ShayanTalaei/CHESS ; original LICENSE retained.
- BIRD / Spider benchmark metadata and project-derived examples: follow the respective original dataset terms. Original SQLite databases and pretrained model weights are obtained separately.

The source directories are vendored rather than Git submodules so project-specific tool and reward changes are included. They are snapshots, not claims to be current upstream versions. This document does not grant a new license over third-party artifacts.
