var xtend = require('xtend')
var flockRevelation = require('@flockcore/revelation')
var defaults = require('@flockcore/presets')

module.exports = flock

function flock (vaultr, opts) {
  var port = (opts && opts.port) || 3282
  var flockOpts = xtend({
    hash: false,
    stream: function (opts) {
      return vaultr.replicate(opts)
    }
  }, opts)

  var sw = flockRevelation(defaults(flockOpts))

  vaultr.on('changes', function (ddb) {
    sw.join(ddb.revelationKey)
  })

  vaultr.on('add', function (ddb) {
    sw.join(ddb.revelationKey)
  })

  vaultr.on('remove', function (ddb) {
    sw.leave(ddb.revelationKey)
  })

  sw.listen(port)
  sw.once('error', function () {
    sw.listen()
  })

  return sw
}
