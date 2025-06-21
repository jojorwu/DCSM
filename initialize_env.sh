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

if [ -f "./generate_grpc_code.sh" ]; then
    ./generate_grpc_code.sh || { echo "Ошибка при генерации gRPC кода с помощью generate_grpc_code.sh. Убедитесь, что `protoc` установлен и скрипт работает корректно."; exit 1; }
else
    echo "Скрипт 'generate_grpc_code.sh' не найден. Пропуск генерации gRPC кода."
fi

echo ""
echo "--------------------------------------------------------"
echo "Инициализация среды разработки DCSM завершена!"
echo "Теперь venv создано и зависимости установлены."
echo "НЕ БЫЛИ ВЫПОЛНЕНЫ команды очистки из оригинального setup_environment.sh."
echo "--------------------------------------------------------"
