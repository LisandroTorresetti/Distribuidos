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

Las colas se van a llamar `eof-stage-city-queue`

## Exchanges

Están en todos lados, la estructura para el nombre es:

`sourceStage-destinationStage-topicName`, por ej:

+ `weather-rainjoiner-topic`
+ `trips-duplicatejoiner-topic`
+ `stations-montrealjoiner-topic`

