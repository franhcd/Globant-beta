## Globant code challenge

Solución propuesta por Francisco Castillo Dahbar.

### Base de datos

La base de datos fue implementada a través del servicio **AWS RDS**, ya que este servicio está orientado a la creación y administración de BD relacionales destinadas al uso transaccional. La instancia elegida comprende el motor MySQL y la clase db.t3.micro (ofrecido por la capa gratuita de AWS).

![](https://33333.cdn.cke-cs.com/kSW7V9NHUXugvhoQeFaf/images/cabf32dd152a8e4fabbba32bfb4dd53f98a04fcdc667f40e.png)

#### Conexión

Para la conexión con la BD opté por disponerla accesible públicamente utilizando un Internet Gateway en la VPC. No obstante, para resguardar la seguridad, creé una regla en el security group asociado a la instancia que permita el acceso únicamente desde mi IP, por el protocolo TCP y el puerto 3306 (MySQL).

![](https://33333.cdn.cke-cs.com/kSW7V9NHUXugvhoQeFaf/images/3e690eee43b9bd9b0a55570c722c3017030677a1496dc26b.png)

#### Tablas

Los ddl de la creación de las tablas se encuentran en ".../Database/DDL Tables"

---

### Move historic data from files in CSV format to the new database

Para este punto opté por utilizar el servicio **AWS Lambda.** Este servicio es serverless, rápido, ágil, se integra muy bien con otros servicios/funcionalidades y es de bajo costo.

Los archivos csv se encuentran en el servicio de Object Storage **AWS S3.**

_El código de esta función lambda y el template de implementación por CloudFormation se encuentran en ".../Lambda/History/"_

---

### Create a REST API service to receive new data

El servicio utilizado fue **AWS APIGateway.** Con este servicio podemos crear una API y administrarla.

Esta api está configurada únicamente con el método POST; y por detrás cada llamada ejecuta una función Lambda de **AWS Lambda**.

_El código de esta función lambda y el template de implementación por CloudFormation se encuentran en ".../Lambda/Post/"_

#### Uso de la API

La URL de la api es la siguiente: [https://hbt1gdr7g3.execute-api.us-east-1.amazonaws.com/Globant-beta](https://hbt1gdr7g3.execute-api.us-east-1.amazonaws.com/Globant-beta)

##### Formato del Header

Como primer requerimiento se necesita la presencia de un header cuyo KEY es ‘table’ y sus posibles VALUES son: '**departments**', ‘**jobs**’, ‘**hired\_employees**’.

##### Formato del Body

La estructura del body debe respetar las siguientes reglas de formato:

*   ‘Content-Type’ de tipo **text/plain.**
*   Filas enteras separadas por salto de línea.
*   Campos separados por coma (,) y sin el uso de comillas simples ni dobles.
*   Para valores NULL, dejar el campo vacío.

Por ejemplo, para una inserción de 3 filas de datos en la tabla “Hired Employees” el body sería el siguiente:

```plaintext
2000, Jose Castro, 2021-07-27T15:12:20Z, 12, 6
2001, Ramon Cepeda, 2022-06-13T16:02:18Z, 5, 2
2002, Candelaria Botta, 2021-08-23T21:14:07Z, , 65
```

Cada llamada a la API que cumpla con el header requerido (aun teniendo errores en el body) es almacenada como registro en un archivo CSV en el servicio **AWS S3.**

![](https://33333.cdn.cke-cs.com/kSW7V9NHUXugvhoQeFaf/images/f149af790c5a67d7654dd2dc81f6554a529b879a3e9070dd.png)

---

### Create a feature to backup for each table and save it in the file system AVRO format

Para esta funcionalidad desarrollé una función Lambda. 

_El código de esta función lambda y el template de implementación por CloudFormation se encuentran en ".../Lambda/Backup/"_

#### Uso de la función

Para realizar un backup de una tabla es necesario pasar como Input un json con el nombre de la misma.

Por ejemplo, para la tabla ‘departments’ el input sería el siguiente:

```plaintext
{ “table”: “departments” }
```

El correspondiente backup se guarda en el servicio de Object Storage **AWS S3**. 

![](https://33333.cdn.cke-cs.com/kSW7V9NHUXugvhoQeFaf/images/6bc9f0a7acc84cc8f14425bfbf8f8e1f76eb6eb6c2180394.png)

---

### Create a feature to restore a certain table with its backup

Para esta funcionalidad desarrollé una función Lambda. 

_El código de esta función lambda y el template de implementación por CloudFormation se encuentran en ".../Lambda/Restore/"_

#### Uso de la función

Para realizar un backup de una tabla es necesario pasar como Input un json con el nombre de la tabla y el URI del objeto de **AWS S3** correspondiente al backup deseado a restaurar.

Por ejemplo, para la tabla ‘departments’ el input sería el siguiente:

```plaintext
{ "table": "departments",
"file": "s3://globant-container/output/department_2023-01-02.avro"}
```

---

### Number of empoyees hired for each job and department in 2021 divided by quarter. 

Para este punto opté por crear una vista en la base de datos. De esta forma, los datos estarán siempre actualizados y la posible comunicación con otras tecnologías se podrá resuelver de manera muy sencilla con la misma conexión a la BD.

_El DDL de la creación de la vista se encuentra en ".../Lambda/Views/View1"_
