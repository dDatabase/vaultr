var vaultr = require('@ddatabase/vaultr')
var ddatabase = require('@ddatabase/core')

var ar = vaultr('./my-vaultr') // also supports passing in a storage provider
var ddb = ddatabase('./my-ddb')

ddb.on('ready', function () {
  ar.add(ddb.key, function () {
    console.log('will now vault the ddb')
  })
})

ar.on('sync', function (ddb) {
  console.log('ddb is synced', ddb.key)
})

// setup replication
var stream = ar.replicate()
stream.pipe(ddb.replicate({live: true})).pipe(stream)

ddb.append(['hello', 'world'])
