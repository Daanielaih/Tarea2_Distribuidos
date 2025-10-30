# Script para crear directorio data si no existe
mkdir -p data

# Crear archivo .gitkeep para mantener el directorio en git
touch data/.gitkeep

# Verificar que test.csv existe
if [ ! -f "data/test.csv" ]; then
    echo "ADVERTENCIA: data/test.csv no existe. Por favor coloca tu archivo CSV allí."
fi

