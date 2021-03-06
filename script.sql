drop table if exists cpe_aux;
create table cpe_aux (
    id int primary key NOT NULL AUTO_INCREMENT,
    tipo_cpe varchar(2),
    serie varchar(4),
    numero varchar(8),
    fecha date,
    codigo_cliente varchar(10),
    tipo_documento varchar(1),
    documento varchar(20),
    razon_social varchar(254),
    direccion varchar(254),
    email varchar(254),
    sub_total decimal(10,2),
    descuento_global decimal(10,2),
    igv decimal(10,2),
    total decimal(10, 2),
    tipo_cpe_nc varchar(2),
    serie_nc varchar(4),
    numero_nc varchar(8),
    fecha_nc date,
    codigo_articulo varchar(13),
    descripcion varchar(254),
    unidad_medida varchar(3),
    cantidad decimal(10,2),
    precio_unitario decimal(10,2),
    sub_total_art decimal(10,2),
    igv_art decimal(10,2),
    total_art decimal(10,2),
    enviado varchar(10)
)
character set utf8 collate utf8_general_ci;
