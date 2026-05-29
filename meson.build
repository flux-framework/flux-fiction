project(
  'flux-fiction',
  'c',
  version: '0.1.0',
  meson_version: '>=1.3.0',
  default_options: [
    'c_std=c99',
    'warning_level=2',
  ],
)

py_mod = import('python')
py = py_mod.find_installation(pure: false)

subdir('src')
