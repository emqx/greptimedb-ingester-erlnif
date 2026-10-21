# Maintenance and release policy

## Branches

- `main` is the active development branch. New features and normal bug fixes
  land here first.
- `release/6.x` is the maintenance branch used by EMQX 6.x. It is cut from the
  EMQX 6.x compatibility point and receives only critical backports. New
  features must not be added to this branch.

## Tags

- EMQX 6.x consumes erlnif `0.1.x` tags. Those tags are created from
  `release/6.x`.
- Feature releases are tagged from `main`. The `0.2.0` tag is the first release
  on that line.

## Backports

- Only severe or critical fixes should be cherry-picked to `release/6.x`.
- Backports must include their regression tests and pass the full Common Test
  suite before a new `0.1.x` tag is created.
