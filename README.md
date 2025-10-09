# PIDS-Proyecto1

**Proyecto PIDS 25/26 – Proyectos de Ingeniería de Sistemas de Datos (UPM)**  
Sistema **procesamiento y almacenamiento de datos** con **MongoDB/Spark** y un **chatbot Rasa** para interactuar con la información procesada.

---

## Descripción general

EdgePoseBot integra tres componentes principales dentro de un flujo completo de ingeniería de datos:

1. **Infraestructura y almacenamiento**  
   - Ingesta y procesamiento de datos con **Python/Spark**.  
   - **Base de datos MongoDB** para almacenar y acceder a los resultados.  
   - Contenedores desplegados con **Docker** y **Docker Compose**.

2. **Interacción conversacional**  
   - Chatbot implementado con **Rasa**, conectado a la infraestructura.  
   - Permite consultar información y ejecutar acciones mediante lenguaje natural.

## Tecnologías utilizadas

| Componente | Tecnología |
|-------------|-------------|
| Almacenamiento | MongoDB |
| Procesamiento distribuido | Apache Spark |
| Chatbot | Rasa |
| Despliegue | Docker, Docker Compose |
| Desarrollo | Python, VS Code, Google Colab |

---

## Instalación y ejecución

1. **Clonar el repositorio**
   ```bash
   git clone https://github.com/<usuario>/PIDS-Entrega1.git
   cd PIDS-Entrega1
   ```

2. **Ejecutar el Docker Compose**
   ```bash
   docker compose up --build
   ```
2. **Entrar al Chatbot de Rasa**
   ```motor de búsqueda
   localhost:8080
   ```



