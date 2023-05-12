# Summary

## Routing keys

En **todas** tenemos que incluir la _city_. La estructura va a ser:

`stage.ciy.ID`

+ `stage`: hace referencia al lugar en dónde queremos mandar el mensaje, por ej puede ser algun filter, joiner, etc. Ejs: `weather, trips, stations, rainjoiner`
+  `city`: los posibles valores son: `montreal, toronto o washington`. **OBS:** respetar el formato, NO usar mayusculas
+ `ID`: algún número que permita identificar al nodo, en ciertos casos puede ser una mezcla de IDs, o sea algo de la pinta `idType1.idType2`

En todos los casos para usar una **routing key genérica**, lo único que se reemplaza es la parte del ID, ej: `weather.toronto.*`, de esta forma a todos los workers de Toronto relacionados al _weather_ van a recibir el EOF.

## EOF messages

Me gusta pensarlo como 'eu, avisale a la etapa X que ya recibió todos los datos', en ese caso el mensaje tiene la siguiente forma:
`eof.stageX.city`. Este mensaje (salvo en el caso del _server_) es enviado al EOF Manager.

### EOF Manager

Tendrá un tipo de 'contador' para saber cuántos EOFs espera de alguna etapa en particular. Cada etapa está compuesta por ciudades, esto nos va a permitir que las cantidades de nodos para cada ciudad puedan ser distintas si es que queremos eso.

Va a tener una única cola de acceso, la cual se va a llamar `eof-queue`. Esta cola va a recibir los distintos _eof_ que pueden venir del sistema y dado que la estructura es conocida, va a saber a que contador le tiene que actualizar el valor.

En caso de que un contador llegue a cero, o sea ya recibió todos los EOF que esperaba, lo que hace es notificarle a la etapa correspondiente que ya puede arrancar a procesar, para esto lo que se hace es mandar el mensaje de EOF que se recibió pero con una _routing key_ genérica así le llega a todas las colas de un determinado _exchange_. Por ejemplo: si estamos en la etapa de _joinear_ los datos relacionados al _weather_ de Toronto, la routing key será `eof.weather.toronto`, de esta forma todos los nodos de Toronto sabrán que ya pueden arrancar a procesar los _trips_ para poder hacer el join correspondiente.

La ventaja de que el _EOF Manager_ encole un mensaje con una routing key especial es que no vamos a tener delays para las ciudades, o sea su procesamiento se puede realizar de forma independiente.

## Exchanges

Están en todos lados, la idea principal es que entre diferentes etapas se comuniquen utilizando exchanges, mientras que con el _EOF Manager_ a través de una cola directa. La estructura para el nombre de cada uno de los _exchanges_ será:

`sourceStage-destinationStage-topicName`

Por ej:

+ `weather-rainjoiner-topic`
+ `trips-duplicatejoiner-topic`
+ `stations-montrealjoiner-topic`


