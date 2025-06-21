#!/bin/bash

# --- Базовая настройка среды и навигация ---
# set -e: Немедленный выход при ошибке.
# set -u: Выход при использовании неопределенной переменной.
# set -x: Печатать команды и их аргументы по мере их выполнения (для отладки).
set -eux

# Переход в корневой каталог проекта
cd /app

# --- 1. Настройка Python окружения ---
echo "--- Настройка Python окружения ---"

# Создание и активация виртуального окружения для изоляции зависимостей
python3 -m venv venv
source venv/bin/activate

# Переменная для изоляции pip от системных пакетов
export PIP_BREAK_SYSTEM_PACKAGES=1

# Поиск и установка Python зависимостей из всех файлов requirements.txt
# (Поиск в текущем каталоге и на один уровень вглубь)
echo "Поиск и установка Python зависимостей из requirements.txt..."
files=$(find . -maxdepth 2 -type f -wholename "*requirements*.txt")

# Проверяем, найдены ли файлы requirements.txt, и устанавливаем зависимости
if [ -n "$files" ]; then
    python -m pip install $(echo "$files" | xargs -I {{}} echo -r {{}}) || { echo "Ошибка при установке Python зависимостей. Проверьте requirements.txt"; exit 1; }
else
    echo "Файлы 'requirements.txt' не найдены. Пропуск установки Python зависимостей."
fi

# --- 2. Настройка Node.js окружения (если папка 'dcs_memory_node' существует) ---
echo ""
echo "--- Настройка Node.js окружения (если папка 'dcs_memory_node' существует) ---"

# Проверяем наличие папки для Node.js компонентов
if [ -d "dcs_memory_node" ]; then
    echo "Переход в 'dcs_memory_node' для установки Node.js зависимостей..."
    cd dcs_memory_node

    # Инициализация Node.js проекта, если еще не инициализирован (npm init -y)
    # Используем '|| true' чтобы не прерывать скрипт, если package.json уже есть
    npm init -y || true

    echo "Установка Node.js зависимостей..."
    npm install || { echo "Ошибка при установке Node.js зависимостей. Проверьте package.json в dcs_memory_node"; exit 1; }

    echo "Возвращение в корневой каталог проекта..."
    cd ..
else
    echo "Папка 'dcs_memory_node' не найдена. Пропуск установки Node.js зависимостей."
fi

# --- 3. Генерация gRPC Кода ---
echo ""
echo "--- Генерация gRPC кода для Python и Node.js ---"

# Предполагается, что ваш скрипт generate_grpc_code.sh обрабатывает генерацию для обоих языков.
# Убедитесь, что `protoc` (Protocol Buffers compiler) установлен и доступен в PATH.
# Если `generate_grpc_code.sh` не работает или его нужно настроить,
# вот примеры ручной генерации (закомментированы):
#
# # Для Python:
# python3 -m grpc_tools.protoc -I. --python_out=generated_grpc --grpc_python_out=generated_grpc dcs_memory/common/grpc_protos/*.proto
#
# # Для Node.js (если `dcs_memory_node` существует и имеет `grpc-tools` в devDependencies):
# if [ -d "dcs_memory_node" ]; then
#     cd dcs_memory_node
#     ./node_modules/.bin/grpc_tools_node_protoc \
#         --plugin=protoc-gen-grpc=`which grpc_tools_node_protoc_plugin` \
#         --js_out=import_style=commonjs,binary:./generated_protos \
#         --grpc_out=grpc_js:./generated_protos \
#         -I../dcs_memory/common/grpc_protos \
#         ../dcs_memory/common/grpc_protos/*.proto
#     cd ..
# fi

if [ -f "./generate_grpc_code.sh" ]; then
    ./generate_grpc_code.sh || { echo "Ошибка при генерации gRPC кода с помощью generate_grpc_code.sh. Убедитесь, что `protoc` установлен и скрипт работает корректно."; exit 1; }
else
    echo "Скрипт 'generate_grpc_code.sh' не найден. Пропуск генерации gRPC кода."
fi

echo ""
echo "--------------------------------------------------------"
echo "Настройка среды разработки DCSM завершена!"
echo "Теперь вы можете запускать тесты или сервисы."
echo "--------------------------------------------------------"

# --- Дополнительные команды для запуска тестов и сервисов (информационно) ---
# Эти команды не запускаются автоматически этим скриптом,
# их нужно выполнять вручную в отдельных терминалах.

echo ""
echo "--- Как запускать тесты: ---"
echo "  Для Python-тестов:               python3 -m pytest"
echo "  Для Node.js-тестов (из папки dcs_memory_node):"
echo "    cd dcs_memory_node && npm test && cd .."

echo ""
echo "--- Как запускать сервисы (в отдельных терминалах): ---"
echo "  Запуск GLM (Python):            python3 dcs_memory/services/glm/app/main.py"
echo "  Запуск SWM (Python):            python3 dcs_memory/services/swm/app/main.py"
echo "  Запуск примера агента (Python): python3 example.py"
echo "  Запуск Node.js сервиса (из папки dcs_memory_node):"
echo "    cd dcs_memory_node && node src/index.js && cd .."

echo ""
echo "--- Команды для отладки и обслуживания: ---"
echo "  Просмотр запущенных Python-процессов: pgrep -lf python"
echo "  Просмотр логов сервиса (пример):       tail -f /path/to/your/service/log/file.log"
echo "  Очистка сгенерированных файлов и кэша:"
echo "    rm -rf generated_grpc generated_grpc_code"
echo "    find . -name \"__pycache__\" -type d -exec rm -rf {} +"
echo "    find . -name \"*.pyc\" -type f -delete"
echo "    rm -rf venv"
echo "    if [ -d \"dcs_memory_node\" ]; then rm -rf dcs_memory_node/node_modules; fi"
