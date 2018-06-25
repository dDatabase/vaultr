var tape = require('tape')
var dwRem = require('@dwcore/rem')
var ddrive = require('@ddrive/core')
var tmp = require('temporary-directory')
var vaultr = require('../')

tape('dDatabase Vaultr Tests: add vault', function (t) {
  var vault = ddrive(dwRem)

  vault.writeFile('/hello.txt', 'world', function () {
    var a = vaultr(dwRem)

    a.add(vault.key, function () {
      var stream = vault.replicate()
      stream.pipe(a.replicate()).pipe(stream)
    })

    a.on('sync', function () {
      t.pass('synced')
      a.get(vault.key, function (_, metadata, content) {
        t.pass('has metadata', !!metadata)
        t.pass('has content', !!content)
        t.end()
      })
    })
  })
})

tape('dDatabase Vaultr Tests: list vaults', function (t) {
  var vault = ddrive(dwRem)

  vault.writeFile('/hello.txt', 'world', function () {
    var a = vaultr(dwRem)

    a.add(vault.key, function () {
      a.list(function (err, list) {
        t.error(err, 'no error')
        t.same(list, [vault.key])
        t.end()
      })
    })
  })
})

tape('dDatabase Vaultr Tests: list vaults on disk', function (t) {
  var vault = ddrive(dwRem)
  var vault2 = ddrive(dwRem)

  tmp(function (_, dir, cleanup) {
    vault2.writeFile('/hello.txt', 'world', function () { })
    vault.writeFile('/hello.txt', 'world', function () {
      var a = vaultr(dir)

      a.add(vault.key, function () {
        a.add(vault2.key, function () {
          a.list(function (err, list) {
            t.error(err, 'no error')
            t.same(list.length, 2)

            var a2 = vaultr(dir)
            a2.list(function (err, list) {
              t.error(err, 'no error')
              t.same(list.length, 2)
              cleanup(function () {
                t.end()
              })
            })
          })
        })
      })
    })
  })
})
