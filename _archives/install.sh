#!/bin/bash
wget https://bootstrap.pypa.io/get-pip.py
python3 get-pip.py
alias pip=~/.local/bin/pip
pip install pipenv
alias pipenv=~/.local/bin/pipenv
rm get-pip.py
pipenv shell
alias pipenv=~/.local/bin/pipenv
# установка пакетов из .lock (заданные версии) без обновления версий окружения
pipenv install
#pipenv run pip install psycopg2-binary
#pipenv run pip install python-telegram-bot
mkdir migrations/tmp sandbox/tmp configs/exchange/private configs/signals/private
chmod 777 -Rf migrations/tmp sandbox/tmp configs/exchange/private configs/signals/private

# docker-compose environment

make start

python migrations/execute.py