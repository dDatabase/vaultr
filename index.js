var ddatabase = require('@ddatabase/core')
var protocol = require('@ddatabase/protocol')
var path = require('path')
var raf = require('@dwcore/ref')
var dethunk = require('@dwcore/dethunk')
var toBuffer = require('to-buffer')
var util = require('util')
var events = require('events')
var datcat = require('@dwcore/dcat')
var dWebChannel = require('@dwcore/channel')
var dWebStreams2 = require('@dwcore/dws2')
var debug = require('debug')('@ddatabase/vaultr')

module.exports = Vaultr

function Vaultr (storage, key, opts) {
  if (!(this instanceof Vaultr)) return new Vaultr(storage, key, opts)
  events.EventEmitter.call(this)

  if (key && typeof key !== 'string' && !Buffer.isBuffer(key)) {
    opts = key
    key = null
  }
  if (!opts) opts = {}

  var self = this

  this.storage = defaultStorage(storage)
  this.changes = ddatabase(this.storage.changes, key, {valueEncoding: 'json'})
  this.thin = !!opts.thin
  this.ddbs = {}
  this.vaults = {}
  this._ondownload = ondownload
  this._onupload = onupload
  this.ready = dethunk(open)
  this.ready()

  function ondownload (index, data, peer) {
    self.emit('download', this, index, data, peer)
  }

  function onupload (index, data, peer) {
    self.emit('upload', this, index, data, peer)
  }

  function open (cb) {
    self._open(cb)
  }
}

util.inherits(Vaultr, events.EventEmitter)

Vaultr.prototype._open = function (cb) {
  var self = this
  var latest = {}
  var i = 0

  this.changes.createReadStream()
    .on('data', ondata)
    .on('error', cb)
    .on('end', onend)

  function ondata (data) {
    i++
    if (data.type === 'add') latest[data.key] = true
    else delete latest[data.key]
  }

  function onend () {
    self.emit('changes', self.changes)
    if (!self.changes.writable) self._tail(i)

    var keys = Object.keys(latest)
    loop(null)

    function loop (err) {
      if (err) return cb(err)
      var next = keys.length ? toBuffer(keys.shift(), 'hex') : null
      debug('open changes key', next && next.toString('hex'))
      if (next) return self._add(next, loop)
      self.emit('ready')
      cb(null)
    }
  }
}

Vaultr.prototype._tail = function (i) {
  var self = this

  self.changes.createReadStream({live: true, start: i})
    .on('data', function (data) {
      if (data.type === 'add') self._add(toBuffer(data.key, 'hex'))
      else self._remove(toBuffer(data.key, 'hex'))
    })
    .on('error', function (err) {
      self.emit('error', err)
    })
}

Vaultr.prototype.list = function (cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)

    var keys = []
    var a = Object.keys(self.ddbs)
    var b = Object.keys(self.vaults)
    var i = 0

    debug('list, ddbs length', a.length)
    debug('list, vault length', b.length)

    for (i = 0; i < a.length; i++) keys.push(self.ddbs[a[i]].key)
    for (i = 0; i < b.length; i++) keys.push(self.vaults[b[i]].metadata.key)

    cb(null, keys)
  })
}

Vaultr.prototype.get = function (key, cb) {
  var self = this
  key = toBuffer(key, 'hex')
  this.ready(function (err) {
    if (err) return cb(err)

    var dk = ddatabase.revelationKey(key).toString('hex')
    var vault = self.vaults[dk]
    if (vault) return cb(null, vault.metadata, vault.content)

    var ddb = self.ddbs[dk]
    if (ddb) return cb(null, ddb)

    cb(new Error('Could not find ddb'))
  })
}

Vaultr.prototype.add = function (key, cb) {
  if (!cb) cb = noop
  var self = this

  key = toBuffer(key, 'hex')
  this.ready(function (err) {
    if (err) return cb(err)
    if (!self._add(key)) return cb(null)
    self.changes.append({type: 'add', key: key.toString('hex')}, cb)
  })
}

Vaultr.prototype.remove = function (key, cb) {
  if (!cb) cb = noop
  var self = this

  key = toBuffer(key, 'hex')
  this.ready(function (err) {
    if (err) return cb(err)
    if (self._remove(key, cb)) {
      self.changes.append({type: 'remove', key: key.toString('hex')})
    }
  })
}

Vaultr.prototype.status = function (key, cb) {
  if (!cb) cb = noop
  var self = this

  self.ready(function (err) {
    if (err) return cb(err)
    self.get(key, function (err, ddb, content) {
      if (err) return cb(err)
      if (content && content.length === 0 && ddb.length > 1) {
        return content.update(function () {
          self.status(key, cb)
        })
      }
      if (!content) content = {length: 0}
      var need = ddb.length + content.length
      var have = need - blocksRemain(ddb) - blocksRemain(content)
      return cb(null, { key: key, need: need, have: have })
    })

    function blocksRemain (ddb) {
      if (!ddb.length) return 0
      var remaining = 0
      for (var i = 0; i < ddb.length; i++) {
        if (!ddb.has(i)) remaining++
      }
      return remaining
    }
  })
}

Vaultr.prototype.import = function (key, cb) {
  if (!cb) cb = noop
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)
    var keys = {}
    var ddbs = datcat(key)
    var collect = function (item, enc, next) {
      try { item = JSON.parse(item) } catch (e) { return next(e) }
      if (!item.type && !item.key) return next(new Error('ddb does not contain importable keys'))
      if (item.type && item.type === '@ddrive/core') return next(new Error('ddb is a ddrive, not importable'))
      if (item.type === 'add') keys[item.key] = true
      if (item.type === 'remove') delete keys[item.key]
      next()
    }
    dWebChannel(ddbs, dWebStreams2.obj(collect), function (err) {
      if (err) return cb(err)
      var list = Object.keys(keys)
      if (list.length === 0) return cb(new Error('got zero ddbs'))
      function next () {
        var item = list.shift()
        if (!item) return cb()
        self.add(item, function (err) {
          if (err) return cb(err)
          next()
        })
      }
      next()
    })
  })
}

Vaultr.prototype.replicate = function (opts) {
  if (!opts) opts = {}

  if (opts.revelationKey) opts.revelationKey = toBuffer(opts.revelationKey, 'hex')
  if (opts.key) opts.revelationKey = ddatabase.revelationKey(toBuffer(opts.key, 'hex'))

  var stream = protocol({live: true, id: this.changes.id, encrypt: opts.encrypt})
  var self = this

  stream.on('ddb', add)
  if (opts.channel || opts.revelationKey) add(opts.channel || opts.revelationKey)

  function add (dk) {
    self.ready(function (err) {
      if (err) return stream.destroy(err)
      if (stream.destroyed) return

      var hex = dk.toString('hex')
      var changesHex = self.changes.revelationKey.toString('hex')

      var vault = self.vaults[hex]
      if (vault) return onvault()

      var ddb = changesHex === hex ? self.changes : self.ddbs[hex]
      if (ddb) return onddb()

      function onvault () {
        vault.metadata.replicate({
          stream: stream,
          live: true
        })
        vault.content.replicate({
          stream: stream,
          live: true
        })
      }

      function onddb () {
        if (stream.destroyed) return

        stream.on('close', onclose)
        stream.on('end', onclose)

        ddb.on('_vault', onvault)
        ddb.replicate({
          stream: stream,
          live: true
        })

        function onclose () {
          ddb.removeListener('_vault', onvault)
        }

        function onvault () {
          if (stream.destroyed) return

          var content = self.vaults[hex].content
          content.replicate({
            stream: stream,
            live: true
          })
        }
      }
    })
  }

  return stream
}

Vaultr.prototype._remove = function (key, cb) {
  var dk = ddatabase.revelationKey(key).toString('hex')

  var ddb = this.ddbs[dk]

  if (ddb) {
    delete this.ddbs[dk]
    this.emit('remove', ddb)
    ddb.removeListener('download', this._ondownload)
    ddb.removeListener('upload', this._onupload)
    ddb.close(cb)
    return true
  }

  var vault = this.vaults[dk]
  if (!vault) {
    cb()
    return false
  }

  delete this.vaults[dk]
  this.emit('remove', vault.metadata, vault.content)
  vault.metadata.removeListener('download', this._ondownload)
  vault.metadata.removeListener('upload', this._onupload)
  vault.content.removeListener('download', this._ondownload)
  vault.content.removeListener('upload', this._onupload)
  vault.metadata.close(function () {
    vault.content.close(cb)
  })

  return true
}

Vaultr.prototype._add = function (key, cb) {
  var dk = ddatabase.revelationKey(key).toString('hex')
  var self = this
  var emitted = false
  var content = null
  var vault = null

  debug('_add, exists:', this.ddbs[dk] || this.vaults[dk])
  if (this.ddbs[dk] || this.vaults[dk]) return false

  var ddb = this.ddbs[dk] = ddatabase(storage(key), key, {thin: this.thin})
  var downloaded = false

  ddb.on('download', ondownload)
  ddb.on('download', this._ondownload)
  ddb.on('upload', this._onupload)

  ddb.ready(function (err) {
    if (err) return
    if (!ddb.has(0)) emit()

    ddb.get(0, function (_, data) {
      var contentKey = parseContentKey(data)

      if (!contentKey) {
        ddb.on('sync', onsync)
        emit()
        if (isSynced(ddb) && downloaded) onsync()
        return
      }

      debug('_add, was removed', self.ddbs[dk] !== ddb)
      if (self.ddbs[dk] !== ddb) return // was removed

      content = ddatabase(storage(contentKey), contentKey, {thin: self.thin})
      content.on('download', ondownload)
      content.on('download', self._ondownload)
      content.on('upload', self._onupload)
      content.on('sync', onsync)
      ddb.on('sync', onsync)

      delete self.ddbs[dk]
      vault = self.vaults[dk] = {
        metadataSynced: isSynced(ddb),
        metadata: ddb,
        contentSynced: isSynced(content),
        content: content
      }

      ddb.emit('_vault')
      self.emit('add-vault', ddb, content)
      emit()
      if (vault.metadataSynced && vault.contentSynced && downloaded) onsync()
    })
  })

  return true

  function emit () {
    if (emitted) return
    emitted = true
    self.emit('add', ddb, content)
    if (cb) cb()
  }

  function ondownload () {
    downloaded = true
    if (!vault) return
    if (this === ddb) vault.metadataSynced = false
    else vault.contentSynced = false
  }

  function onsync () {
    if (vault) {
      if (self.vaults[dk] !== vault) return
      if (this === content) vault.contentSynced = true
      else vault.metadataSynced = true
      if (vault.metadataSynced && vault.contentSynced) self.emit('sync', ddb, content)
    } else {
      if (self.ddbs[dk] !== ddb) return
      self.emit('sync', ddb, null)
    }
  }

  function storage (key) {
    var dk = ddatabase.revelationKey(key).toString('hex')
    var prefix = dk.slice(0, 2) + '/' + dk.slice(2, 4) + '/' + dk.slice(4) + '/'

    return function (name) {
      return self.storage.ddbs(prefix + name)
    }
  }
}

function noop () {}

function isSynced (ddb) {
  if (!ddb.length) return false
  for (var i = 0; i < ddb.length; i++) {
    if (!ddb.has(i)) return false
  }
  return true
}

function defaultStorage (st) {
  if (typeof st === 'string') {
    return {
      changes: function (name) {
        return raf(path.join(st, 'changes', name))
      },
      ddbs: function (name) {
        return raf(path.join(st, 'ddbs', name))
      }
    }
  }

  if (typeof st === 'function') {
    return {
      changes: function (name) {
        return st('changes/' + name)
      },
      ddbs: function (name) {
        return st('ddbs/' + name)
      }
    }
  }

  return st
}

function parseContentKey (data) {
  var hex = data && data.toString('hex')
  if (!hex || hex.indexOf(toBuffer('@ddrive/core').toString('hex')) === -1 || hex.length < 64) return
  return toBuffer(hex.slice(-64), 'hex')
}
