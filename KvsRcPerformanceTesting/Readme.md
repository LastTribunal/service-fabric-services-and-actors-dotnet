## PerformanceTestingApp
- Sample app that which contains KVS and RC Actor services that undergo migration.
- There is also a WebService that is used to perform different functions on the Actor Services. The WebServices listens on port 9033 and has multiple endpoints as defined in MigrationController.cs.
- The 2 Actor Services and the WebService are all placed on different Node types (as defined in the respective ServiceManifest.xml files).
- For migration, the KVS states are populated by -
  - Making a POST request to /api/migration/generateActors/{numActors}/{numStatesPerActors} on the WebService. 100K actors with 100 states make 1GB of KVS state. To get a larger state, the number of actors andthe number of states per actors can be increased accordingly.
  - Having actor states backed up on a storage account and invoke data loss on kvs actor service partition which will try to restore the actor states from the storage account. (check KVSActorService.OnDataLossAsync method)