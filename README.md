# PIDS-Proyecto1

**Proyecto PIDS 25/26 – Proyectos de Ingeniería de Sistemas de Datos (UPM)**  
Sistema que combina **reconocimiento de gestos con MediaPipe y CNN en Raspberry Pi**, **procesamiento y almacenamiento de datos** con **MongoDB/Spark**, y un **chatbot Rasa** para interactuar con la información procesada.

---

## Descripción general

EdgePoseBot integra tres componentes principales dentro de un flujo completo de ingeniería de datos:

1. **Captura y procesamiento**  
   - Reconocimiento de gestos de mano mediante **MediaPipe**.  
   - Clasificación con una **red neuronal convolucional (CNN)** entrenada en Google Colab.  
   - Ejecución local en **Raspberry Pi (Edge AI)** para preservar la privacidad y optimizar el rendimiento.

2. **Infraestructura y almacenamiento**  
   - Ingesta y procesamiento de datos con **Python/Spark**.  
   - **Base de datos MongoDB** para almacenar y acceder a los resultados.  
   - Contenedores desplegados con **Docker** y **Docker Compose**.

3. **Interacción conversacional**  
   - Chatbot implementado con **Rasa**, conectado a la infraestructura.  
   - Permite consultar información y ejecutar acciones mediante lenguaje natural.

## Tecnologías utilizadas

| Componente | Tecnología |
|-------------|-------------|
| Procesamiento de gestos | MediaPipe, TensorFlow/Keras |
| Dispositivo Edge | Raspberry Pi 3B+ |
| Almacenamiento | MongoDB |
| Procesamiento distribuido | Apache Spark |
| Chatbot | Rasa |
| Despliegue | Docker, Docker Compose |
| Desarrollo | Python, VS Code, Google Colab |

---

## Instalación y ejecución

1. **Clonar el repositorio**
   ```bash
   git clone https://github.com/<usuario>/PIDS-Proyecto1.git
   cd <repositorio>
