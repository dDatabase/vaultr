var tape = require('tape')
var dwRem = require('@dwcore/rem')
var ddatabase = require('@ddatabase/core')
var vaultr = require('../')

tape('dDatabase Vaultr Tests: add ddb', function (t) {
  var ddb = ddatabase(dwRem)
  var a = vaultr(dwRem)

  ddb.append(['hello', 'world'], function () {
    a.once('sync', function () {
      t.pass('should sync')
      a.once('sync', function () {
        t.pass('should sync again')
        t.end()
      })
      ddb.append(['a', 'b', 'c'])
    })

    a.add(ddb.key, function () {
      var stream = ddb.replicate({live: true})
      stream.pipe(a.replicate()).pipe(stream)
    })
  })
})

tape('dDatabase Vaultr Tests: add ddb and replicate to other', function (t) {
  var ddb = ddatabase(dwRem)
  var a = vaultr(dwRem)

  ddb.append(['hello', 'world', 'a', 'b', 'c'], function () {
    a.add(ddb.key, function () {
      var stream = ddb.replicate({live: true})
      stream.pipe(a.replicate()).pipe(stream)
    })
  })

  a.on('add', function () {
    var fork = ddatabase(dwRem, ddb.key)

    var stream = fork.replicate({live: true})
    stream.pipe(a.replicate()).pipe(stream)

    fork.on('sync', function () {
      t.pass('fork synced')
      t.end()
    })
  })
})

tape('dDatabase Vaultr Tests: changes replicate', function (t) {
  var a = vaultr(dwRem)

  a.ready(function () {
    var b = vaultr(dwRem, a.changes.key)

    var stream = a.replicate()
    stream.pipe(b.replicate({key: a.changes.key})).pipe(stream)

    var ddb = ddatabase(dwRem)

    ddb.on('ready', function () {
      a.add(ddb.key)
      b.on('add', function () {
        t.pass('added ddb')
        t.end()
      })
    })
  })
})

tape('dDatabase Vaultr Tests: list ddbs', function (t) {
  var ddb = ddatabase(dwRem)

  ddb.append('a', function () {
    var a = vaultr(dwRem)

    a.add(ddb.key, function () {
      a.list(function (err, list) {
        t.error(err, 'no error')
        t.same(list, [ddb.key])
        t.end()
      })
    })
  })
})
