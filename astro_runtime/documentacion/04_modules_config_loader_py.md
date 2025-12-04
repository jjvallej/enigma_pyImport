# Documentación: modules/config_loader.py

## 1. Información General

El archivo "modules/config_loader.py" es un módulo Python que se encarga de buscar, cargar y parsear el archivo "config.yaml", convirtiéndolo en un objeto Python accesible mediante notación de puntos. Este módulo es fundamental para el sistema ya que proporciona la funcionalidad de carga de configuración que es usada por "config.py" y otros módulos. Su propósito principal es abstraer la complejidad de encontrar y cargar el archivo de configuración en diferentes entornos, funcionando tanto en desarrollo local como en Cloud Composer.

## 2. Propósito y Funcionalidad

El archivo "config_loader.py" cumple varias funciones críticas en el sistema. En primer lugar, busca el archivo "config.yaml" en múltiples ubicaciones posibles, incluyendo rutas relativas para desarrollo local y rutas comunes en Cloud Composer. Esto asegura que el archivo se encuentre independientemente de dónde se ejecute el código.

En segundo lugar, carga y parsea el archivo YAML usando la librería "yaml", convirtiendo el contenido en un diccionario de Python. Luego convierte recursivamente este diccionario en un objeto "SimpleNamespace" que permite acceso mediante notación de puntos, como "config.environments.dev.project_id" en lugar de "config['environments']['dev']['project_id']".

En tercer lugar, proporciona manejo de errores robusto, mostrando mensajes descriptivos si el archivo no se encuentra o si el YAML es inválido. Esto ayuda a diagnosticar problemas de configuración rápidamente.

Finalmente, expone una instancia global de configuración que se carga automáticamente cuando se importa el módulo, permitiendo que otros módulos accedan a la configuración simplemente importando "config_loader.config".

## 3. Estructura del Archivo

El archivo está organizado en tres funciones principales. La función "find_config_file" busca el archivo "config.yaml" en múltiples ubicaciones posibles. La función "load_config" carga y parsea el archivo YAML, convirtiéndolo en un objeto Python. La función "_dict_to_namespace" es una función helper que convierte recursivamente diccionarios en objetos "SimpleNamespace". Al final del archivo se crea una instancia global "config" que se carga automáticamente.

## 4. Función find_config_file

La función "find_config_file" busca el archivo "config.yaml" en múltiples ubicaciones posibles, funcionando tanto en desarrollo local como en Cloud Composer. Esta función no toma parámetros y retorna la ruta completa del archivo si lo encuentra, o lanza una excepción "FileNotFoundError" si no lo encuentra en ninguna ubicación.

La función construye una lista "possible_paths" que contiene todas las rutas posibles donde podría estar el archivo. Primero agrega la ruta relativa desde el archivo actual, calculando el directorio del archivo usando "os.path.dirname" y "os.path.abspath", luego obteniendo el directorio padre, y finalmente construyendo la ruta "config/config.yaml" relativa a ese directorio padre. Esta es la ruta más común en desarrollo local.

Luego agrega rutas comunes en Cloud Composer con prioridad alta. La primera ruta es "/home/airflow/gcs/dags/gdv_general_dbt_dag/config/config.yaml", que es la ubicación estándar cuando el código está desplegado en Cloud Composer. La segunda ruta usa "os.path.join" para construir la misma ruta de manera más robusta. La tercera ruta es un fallback "/home/airflow/gcs/dags/config/config.yaml" por si el archivo está en la raíz de dags en lugar de dentro de "gdv_general_dbt_dag".

Después intenta obtener el directorio de trabajo actual usando "os.getcwd" dentro de un bloque try-except para manejar casos donde no se pueda obtener. Si tiene éxito, agrega rutas relativas al directorio de trabajo actual, incluyendo "config/config.yaml" y "gdv_general_dbt_dag/config/config.yaml".

Luego busca en todos los directorios en "sys.path", que es la lista de directorios donde Python busca módulos. Para cada path en "sys.path", agrega rutas posibles incluyendo el path directamente, el directorio padre del path, y rutas dentro de "gdv_general_dbt_dag" si existe. Esto ayuda a encontrar el archivo cuando se importa desde diferentes ubicaciones.

Finalmente, busca recursivamente desde el directorio actual hacia arriba hasta cinco niveles, agregando rutas "config/config.yaml" y "gdv_general_dbt_dag/config/config.yaml" en cada nivel. Esto permite encontrar el archivo incluso si la estructura de directorios es diferente a la esperada.

Después de construir la lista completa de rutas posibles, elimina duplicados manteniendo el orden. Luego itera sobre cada ruta y verifica si el archivo existe usando "os.path.exists". Si encuentra el archivo, imprime un mensaje de debug con la ruta encontrada y retorna esa ruta.

Si no encuentra el archivo en ninguna ubicación, imprime un mensaje de error listando las primeras quince rutas buscadas, marcando cada una con un símbolo de verificación si existe o una cruz si no existe. Luego lanza una excepción "FileNotFoundError" con un mensaje descriptivo que indica dónde se debe colocar el archivo en Cloud Composer y cuántas ubicaciones se buscaron.

## 5. Función load_config

La función "load_config" carga la configuración desde un archivo YAML y la convierte en un objeto accesible mediante notación de puntos. Esta función toma un parámetro opcional "config_path" que es la ruta al archivo de configuración. Si "config_path" es "None", la función llama a "find_config_file" para buscar el archivo automáticamente.

Si se proporciona una ruta, la función imprime un mensaje de debug indicando qué ruta se está usando. Luego verifica que el archivo exista usando "os.path.exists". Si el archivo no existe, lanza una excepción "FileNotFoundError" con un mensaje indicando que el archivo no se encontró en la ruta especificada.

Si el archivo existe, imprime un mensaje de debug confirmando que se encontró. Luego abre el archivo en modo lectura y usa "yaml.safe_load" para parsear el contenido YAML y convertirlo en un diccionario de Python. El uso de "safe_load" en lugar de "load" es una práctica de seguridad que evita la ejecución de código arbitrario que podría estar en el YAML.

Después de cargar el YAML, verifica que el diccionario resultante no sea "None". Si es "None", significa que el archivo está vacío o el YAML es inválido. En este caso, imprime un mensaje de error y lanza una excepción "ValueError" indicando que el archivo está vacío o es YAML inválido.

Si el YAML se carga correctamente, llama a la función "_dict_to_namespace" pasando el diccionario, que convierte recursivamente el diccionario en un objeto "SimpleNamespace". Retorna este objeto, que permite acceso mediante notación de puntos.

## 6. Función _dict_to_namespace

La función "_dict_to_namespace" es una función helper recursiva que convierte diccionarios de Python en objetos "SimpleNamespace", permitiendo acceso mediante notación de puntos en lugar de notación de corchetes. Esta función toma un parámetro "d" que puede ser un diccionario, una lista, o cualquier otro tipo de valor.

Si "d" es un diccionario, la función itera sobre cada clave y valor en el diccionario. Para cada valor, llama recursivamente a "_dict_to_namespace" para convertir también los valores anidados. Esto asegura que todos los diccionarios anidados dentro del diccionario principal también se conviertan en objetos "SimpleNamespace". Después de procesar todos los valores, crea un nuevo objeto "SimpleNamespace" usando "SimpleNamespace(**d)", que convierte el diccionario en un objeto con atributos accesibles mediante notación de puntos.

Si "d" es una lista, la función crea una nueva lista donde cada elemento se convierte recursivamente llamando a "_dict_to_namespace" sobre cada valor. Esto asegura que si hay listas de diccionarios, cada diccionario en la lista también se convierta en un objeto "SimpleNamespace".

Si "d" no es ni un diccionario ni una lista, la función simplemente retorna el valor sin modificar. Esto permite que valores primitivos como strings, números, y booleanos pasen sin cambios.

El resultado de esta conversión es que en lugar de acceder a la configuración como "config['environments']['dev']['project_id']", se puede acceder como "config.environments.dev.project_id", lo cual es más legible y más similar a cómo se accede a atributos de objetos en Python.

## 7. Instancia Global de Configuración

Al final del archivo, después de definir todas las funciones, se crea una instancia global de configuración llamada "config". Esta variable se asigna llamando a "load_config()" sin argumentos, lo que hace que la función busque automáticamente el archivo "config.yaml" y lo cargue.

Esta instancia global se crea cuando el módulo se importa por primera vez, lo que significa que la configuración se carga una sola vez al inicio y está disponible para todos los módulos que importen "config_loader". Esto es eficiente porque evita cargar el archivo múltiples veces.

Otros módulos pueden acceder a esta configuración importando "from modules.config_loader import config" o "import modules.config_loader" y luego usando "modules.config_loader.config". El objeto "config" es un "SimpleNamespace" que contiene toda la configuración del archivo "config.yaml" accesible mediante notación de puntos.

## 8. Cómo se Usa en el Código

El módulo "config_loader.py" se usa principalmente de manera indirecta a través de "config.py". Cuando "config.py" se importa, intenta importar "config_loader" y acceder a "config_loader.config", que es la instancia global de configuración.

También se puede usar directamente importando "from modules.config_loader import config" y luego accediendo a la configuración mediante "config.environments.dev.project_id" o similar. Sin embargo, la forma recomendada es usar "config.py" que proporciona variables globales más convenientes.

La función "find_config_file" rara vez se llama directamente, ya que "load_config" la llama automáticamente si no se proporciona una ruta. Sin embargo, podría ser útil para debugging o para verificar dónde se encuentra el archivo de configuración.

La función "load_config" puede ser llamada directamente si se necesita cargar un archivo de configuración diferente o desde una ruta específica, pasando la ruta como argumento. Esto es útil en casos especiales donde se necesita cargar múltiples archivos de configuración.

## 9. Flujo de Ejecución

Cuando el módulo "config_loader.py" se importa por primera vez, Python ejecuta todo el código a nivel de módulo. Primero se importan las dependencias necesarias: "yaml" para parsear YAML, "os" para operaciones del sistema de archivos, "sys" para acceder a "sys.path", y "SimpleNamespace" de "types" para crear objetos con notación de puntos.

Luego se definen las tres funciones: "find_config_file", "load_config", y "_dict_to_namespace". Estas funciones están disponibles pero no se ejecutan hasta que se llamen.

Finalmente, se ejecuta la línea "config = load_config()", que llama a "load_config" sin argumentos. Esto hace que "load_config" llame a "find_config_file" para buscar el archivo "config.yaml". Una vez encontrado, "load_config" abre el archivo, lo parsea con "yaml.safe_load", y convierte el diccionario resultante en un objeto "SimpleNamespace" usando "_dict_to_namespace". El objeto resultante se asigna a la variable global "config".

Todo este proceso ocurre automáticamente cuando se importa el módulo, por lo que la configuración está disponible inmediatamente después de la importación.

## 10. Manejo de Errores

El código incluye varios mecanismos de manejo de errores. Si "find_config_file" no encuentra el archivo en ninguna ubicación, lanza una excepción "FileNotFoundError" con un mensaje descriptivo que indica dónde se debe colocar el archivo en Cloud Composer y lista las ubicaciones donde se buscó. Esto ayuda a diagnosticar problemas de despliegue rápidamente.

Si "load_config" recibe una ruta pero el archivo no existe en esa ruta, lanza una excepción "FileNotFoundError" indicando la ruta específica donde se esperaba encontrar el archivo.

Si el archivo YAML está vacío o es inválido, "yaml.safe_load" puede retornar "None" o lanzar una excepción. El código verifica si el resultado es "None" y lanza una excepción "ValueError" con un mensaje indicando que el archivo está vacío o es YAML inválido. Si "yaml.safe_load" lanza una excepción por YAML mal formado, esa excepción se propaga normalmente, proporcionando información sobre el error de sintaxis.

Todos estos errores están diseñados para ser informativos y ayudar a diagnosticar problemas de configuración rápidamente. Los mensajes de error incluyen información específica sobre qué salió mal y cómo corregirlo.

## 11. Búsqueda de Archivos en Múltiples Ubicaciones

La función "find_config_file" busca el archivo en muchas ubicaciones diferentes para maximizar las posibilidades de encontrarlo. Esta búsqueda exhaustiva es necesaria porque el código puede ejecutarse en diferentes contextos: desarrollo local con diferentes estructuras de directorios, Cloud Composer con rutas estándar, o en contenedores Docker con montajes de volúmenes.

La búsqueda se realiza en orden de prioridad, verificando primero las rutas más comunes. Si el archivo se encuentra en una de las primeras rutas, se retorna inmediatamente sin verificar las rutas restantes. Esto hace que la búsqueda sea eficiente en los casos más comunes.

Las rutas se buscan en el siguiente orden aproximado: primero rutas relativas desde el archivo actual, luego rutas comunes en Cloud Composer, luego rutas relativas al directorio de trabajo actual, luego rutas en "sys.path", y finalmente búsqueda recursiva hacia arriba. Este orden prioriza las ubicaciones más probables primero.

## 12. Conversión de Diccionarios a SimpleNamespace

La conversión de diccionarios a objetos "SimpleNamespace" es importante porque permite un acceso más natural y legible a la configuración. En lugar de usar notación de corchetes como "config['environments']['dev']['project_id']", se puede usar notación de puntos como "config.environments.dev.project_id".

Esta conversión es recursiva, lo que significa que todos los diccionarios anidados también se convierten. Por ejemplo, si "config.yaml" tiene una estructura anidada como "environments.dev.project_id", el objeto resultante permite acceder a "config.environments.dev.project_id" directamente.

La función también maneja listas correctamente, convirtiendo cada elemento de la lista si es un diccionario. Esto es importante porque "config.yaml" puede contener listas, como en el caso de "evaplan.fuentes" que es una lista de strings, o "idi.link_name_keywords" que también es una lista.

Los valores primitivos como strings, números, y booleanos pasan sin modificación, lo cual es correcto porque no necesitan conversión.

## 13. Mensajes de Debug

El código incluye varios mensajes de debug que se imprimen durante la ejecución. Cuando "find_config_file" encuentra el archivo, imprime "DEBUG: Found config.yaml at:" seguido de la ruta encontrada. Cuando "load_config" usa una ruta, imprime "DEBUG: Using config path:" seguido de la ruta. Cuando "load_config" encuentra el archivo, imprime "DEBUG: Configuration file found at:" seguido de la ruta.

Estos mensajes de debug son útiles para diagnosticar problemas, especialmente cuando el archivo no se encuentra o cuando hay problemas con la carga. Sin embargo, en producción estos mensajes pueden ser verbosos, por lo que podrían ser controlados mediante la configuración "global_config.debug" en "config.yaml".

Si el archivo no se encuentra, se imprimen mensajes de error más detallados que listan todas las rutas donde se buscó, marcando cada una como existente o no existente. Esto proporciona información valiosa para diagnosticar problemas de despliegue.

## 14. Compatibilidad con Diferentes Entornos

El código está diseñado para funcionar en múltiples entornos diferentes. En desarrollo local, el archivo "config.yaml" generalmente está en una ruta relativa al proyecto. En Cloud Composer, el archivo está en "/home/airflow/gcs/dags/gdv_general_dbt_dag/config/config.yaml". En contenedores Docker, puede estar en diferentes ubicaciones dependiendo de cómo se monten los volúmenes.

La búsqueda exhaustiva en múltiples ubicaciones asegura que el archivo se encuentre independientemente del entorno. Esto hace que el código sea más robusto y portátil entre diferentes configuraciones de despliegue.

La función también maneja casos edge como cuando "os.getcwd" falla o cuando "sys.path" contiene valores None o inválidos, usando bloques try-except para evitar que estos casos causen errores fatales.

## 15. Notas Importantes

Es importante entender que la instancia global "config" se crea cuando el módulo se importa por primera vez. Si el archivo "config.yaml" cambia después de que el módulo se ha importado, esos cambios no se reflejarán automáticamente. Para cargar una nueva versión de la configuración, sería necesario recargar el módulo o llamar a "load_config" nuevamente.

La función usa "yaml.safe_load" en lugar de "yaml.load" por razones de seguridad. "safe_load" solo carga tipos de datos básicos de YAML y no ejecuta código arbitrario, lo cual es importante cuando se carga configuración desde fuentes externas.

El objeto "SimpleNamespace" es una clase simple de Python que permite crear objetos con atributos dinámicos. Es más ligero que crear una clase personalizada pero proporciona la funcionalidad necesaria para acceso mediante notación de puntos.

La búsqueda recursiva hacia arriba está limitada a cinco niveles para evitar búsquedas infinitas o muy profundas. Si el archivo no se encuentra en los primeros cinco niveles, se continúa con otras estrategias de búsqueda.

---

Última actualización: 2025-01-XX  
Archivo documentado: "modules/config_loader.py"  
Versión del archivo: 3.0

