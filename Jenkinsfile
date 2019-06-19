livefyre('''
test:
  image:
    label: corpjenkins/polyglot
  git: true
  commands:
    - make clean test
deploy:
  branch: master
  image:
    label: corpjenkins/polyglot
  git: true
  confirm: true
  commands:
    - make clean env
''')
