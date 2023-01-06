## Globant code challenge

Solución propuesta por Francisco Castillo Dahbar.

### Base de datos

La base de datos fue implementada a través del servicio **AWS RDS**, ya que este servicio está orientado a la creación y administración de BD relacionales destinadas al uso transaccional. La instancia elegida comprende el motor MySQL y la clase db.t3.micro (ofrecido por la capa gratuita de AWS).

![](https://33333.cdn.cke-cs.com/kSW7V9NHUXugvhoQeFaf/images/cabf32dd152a8e4fabbba32bfb4dd53f98a04fcdc667f40e.png)

#### Conexión

Para la conexión con la BD opté por disponerla accesible públicamente. No obstante, para resguardar la seguridad, creé una regla en el security group asociado a la instancia que permita el acceso únicamente desde mi IP, por el protocolo TCP y el puerto 3306 (MySQL).

![](https://33333.cdn.cke-cs.com/kSW7V9NHUXugvhoQeFaf/images/3e690eee43b9bd9b0a55570c722c3017030677a1496dc26b.png)

#### Tablas

Los ddl de la creación de las tablas se encuentran en ".../Database/DDL Tables"
