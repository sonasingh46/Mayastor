// Unit tests for nexus grpc api. Nexus is basically a hub which does IO
// replication to connected replicas. We test nexus operations with all
// supported replica types: nvmf, iscsi, bdev, aio and uring. aio is not used
// in the product but it was part of initial implementation, so we keep it in
// case it would be useful in the future. uring was added later and is also
// not used in the product but kept for testing.

'use strict';

const assert = require('chai').assert;
const async = require('async');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const { createClient } = require('grpc-kit');
const grpc = require('grpc');
const common = require('./test_common');
const enums = require('./grpc_enums');
// just some UUID used for nexus ID
const UUID = 'dbe4d7eb-118a-4d15-b789-a18d9af6ff21';
const UUID2 = 'dbe4d7eb-118a-4d15-b789-a18d9af6ff22';

// backend file for aio bdev
const aioFile = '/tmp/aio-backend';
// backend file for io_uring bdev
const uringFile = '/tmp/uring-backend';
// 64MB is the size of nexus and replicas
const diskSize = 64 * 1024 * 1024;
// external IP address detected by common lib
const externIp = common.getMyIp();

const iscsiReplicaPort = '3266';

function wait (ms) {
  var start = new Date().getTime();
  var end = start;
  while (end < start + ms) {
    end = new Date().getTime();
  }
}

// Instead of using mayastor grpc methods to create replicas we use a config
// file to create them. Advantage is that we don't depend on bugs in replica
// code (the nexus tests are more independent). Disadvantage is that we don't
// test the nexus with implementation of replicas which are used in the
// production.
const configNexus = `
[Malloc]
  NumberOfLuns 2
  LunSizeInMB  64
  BlockSize    4096

[iSCSI]
  NodeBase "iqn.2019-05.io.openebs"
  # Socket I/O timeout sec. (0 is infinite)
  Timeout 30
  DiscoveryAuthMethod None
  DefaultTime2Wait 2
  DefaultTime2Retain 60
  ImmediateData Yes
  ErrorRecoveryLevel 0
  # Reduce mem requirements for iSCSI
  MaxSessions 2
  MaxConnectionsPerSession 1

[PortalGroup1]
  Portal GR1 0.0.0.0:${iscsiReplicaPort}

[InitiatorGroup1]
  InitiatorName Any
  Netmask ${externIp}/24

[TargetNode0]
  TargetName "iqn.2019-05.io.openebs:disk1"
  TargetAlias "Backend Malloc1"
  Mapping PortalGroup1 InitiatorGroup1
  AuthMethod None
  UseDigest Auto
  LUN0 Malloc1
  QueueDepth 1
`;

// The config just for nvmf target which cannot run in the same process as
// the nvmf initiator (SPDK limitation).
const configNvmfTarget = `
[Malloc]
  NumberOfLuns 1
  LunSizeInMB  64
  BlockSize    4096

[Nvmf]
  AcceptorPollRate 10000
  ConnectionScheduler RoundRobin

[Transport]
  Type TCP
  # reduce memory requirements
  NumSharedBuffers 32

[Subsystem1]
  NQN nqn.2019-05.io.openebs:disk2
  Listen TCP 127.0.0.1:8420
  AllowAnyHost Yes
  SN MAYASTOR0000000001
  MN NEXUSController1
  MaxNamespaces 1
  Namespace Malloc0 1

# although not used we still have to reduce mem requirements for iSCSI
[iSCSI]
  MaxSessions 1
  MaxConnectionsPerSession 1
`;

function createGrpcClient (service) {
  return createClient(
    {
      protoPath: path.join(
        __dirname,
        '..',
        'rpc',
        'proto',
        'mayastor_service.proto'
      ),
      packageName: 'mayastor_service',
      serviceName: 'Mayastor',
      options: {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
      }
    },
    common.grpc_endpoint
  );
}

// TOMTODO Comment on how these are used as return values from tests in controlPlaneTest
var client;
var uri;

var doUring = (function () {
  var executed = false;
  var supportsUring = false;
  return function () {
    if (!executed) {
      executed = true;
      const { exec } = require('child_process');
      const URING_SUPPORT_CMD = path.join(
        __dirname,
        '..',
        'target',
        'debug',
        'uring-support'
      );
      const CMD = URING_SUPPORT_CMD + ' ' + uringFile;
      exec(CMD, (error) => {
        if (error) {
          return;
        }
        supportsUring = true;
      });
    }
    return supportsUring;
  };
})();

describe('nexus', function () {
  // var client;
  var nbd_device;
  // var iscsi_uri;

  const unpublish = (args) => {
    return new Promise((resolve, reject) => {
      client.unpublishNexus(args, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  };

  const publish = (args) => {
    return new Promise((resolve, reject) => {
      client.publishNexus(args, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  };

  const destroyNexus = (args) => {
    return new Promise((resolve, reject) => {
      client.destroyNexus(args, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  };

  const createNexus = (args) => {
    return new Promise((resolve, reject) => {
      client.createNexus(args, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  };

  const createArgs = {
    uuid: UUID,
    size: 131072,
    children: [
      'nvmf://127.0.0.1:8420/nqn.2019-05.io.openebs:disk2',
      `aio:///${aioFile}?blk_size=4096`
    ]
  };
  this.timeout(50000); // for network tests we need long timeouts

  before((done) => {
    client = createGrpcClient('MayaStor');
    if (!client) {
      return done(new Error('Failed to initialize grpc client'));
    }

    async.series(
      [
        common.ensureNbdWritable,
        (next) => {
          fs.writeFile(aioFile, '', next);
        },
        (next) => {
          fs.truncate(aioFile, diskSize, next);
        },
        (next) => {
          fs.writeFile(uringFile, '', next);
        },
        (next) => {
          fs.truncate(uringFile, diskSize, next);
        },
        (next) => {
          if (doUring()) { createArgs.children.push(`uring:///${uringFile}?blk_size=4096`); }
          next();
        },
        (next) => {
          // Start two spdk instances. The first one will hold the remote
          // nvmf target and the second one everything including nexus.
          // We must do this because if nvme initiator and target are in
          // the same instance, the SPDK will hang.
          //
          // In order not to exceed available memory in hugepages when running
          // two instances we use the -s option to limit allocated mem.
          common.startSpdk(configNvmfTarget, [
            '-r',
            '/tmp/target.sock',
            '-s',
            '128'
          ]);
          common.startMayastor(configNexus, ['-r', common.SOCK, '-s', 386]);

          common.startMayastorGrpc();
          common.waitFor((pingDone) => {
            // use harmless method to test if the mayastor is up and running
            client.listPools({}, pingDone);
          }, next);
        }
      ],
      done
    );
  });

  after((done) => {
    async.series(
      [
        common.stopAll,
        common.restoreNbdPerms,
        (next) => {
          fs.unlink(aioFile, (err) => next());
        },
        (next) => {
          if (doUring()) fs.unlink(uringFile, (err) => next());
          else next();
        }
      ],
      (err) => {
        if (client != null) {
          client.close();
        }
        done(err);
      }
    );
  });

  describe('nvmf', function () {
    const blockFile = '/tmp/test_block'; // TOMTODO Commonise

    const POOL = 'tpool'; // TOMTODO Commonise with replica code

    // TOMTODO Commonise with the replica test??
    function rmBlockFile (done) {
      common.execAsRoot('rm', ['-f', blockFile], (err) => {
        // ignore unlink error
        done();
      });
    }

    // TOMTODO Commonise with the replica test??
    before((done) => {
      const buf = Buffer.alloc(4096, 'm');

      async.series(
        [
          (next) => rmBlockFile(next),
          (next) => fs.writeFile(blockFile, buf, next)
          // (next) => client.createPool({ name: POOL, disks: disks }, next), // TOMTODO Different to replicas
        ],
        done
      );
    });

    it('should create a nexus using only nvmf replica', (done) => {
      const args = {
        uuid: UUID,
        size: diskSize,
        children: [
          'bdev:///Malloc0',
          `aio:///${aioFile}?blk_size=4096`,
          //`iscsi://${externIp}:${iscsiReplicaPort}/iqn.2019-05.io.openebs:disk1`,
          'nvmf://127.0.0.1:8420/nqn.2019-05.io.openebs:disk2'
        ]
      };
      if (doUring()) args.children.push(`uring:///${uringFile}?blk_size=4096`);

      client.CreateNexus(args, done);
    });

    it('should publish the nexus', (done) => {
      client.PublishNexus(
        {
          uuid: UUID,
          share: enums.NEXUS_NVMF
        },
        (err, res) => {
          assert(res.device_path);
          uri = `nvmf://${externIp}:8420/nqn.2019-05.io.openebs:${res.device_path}`; // TOMTODO Should the nvmf target be taking care of this?
          done();
        }
      );
    });

    it('should write to nvmf replica', (done) => {
      const buri = uri;
      console.log('Buri625: ' + buri);
      common.execAsRoot(
        common.getCmdPath('initiator'),
        ['--offset=4096', buri, 'write', blockFile],
        done
      );
    });

    it('should read from nvmf replica', (done) => {
      async.series(
        [
          (next) => {
            // remove the file we used for writing just to be sure that what we read
            // really comes from the replica
            fs.unlink(blockFile, next);
          },
          (next) => {
            common.execAsRoot(
              common.getCmdPath('initiator'),
              ['--offset=4096', uri, 'read', blockFile],
              next
            );
          },
          (next) => {
            fs.readFile(blockFile, (err, data) => {
              if (err) return done(err);
              data = data.toString();
              assert.lengthOf(data, 4096);
              for (let i = 0; i < data.length; i++) {
                if (data[i] != 'm') {
                  next(new Error(`Invalid char '${data[i]}' at offset ${i}`));
                  return;
                }
              }
              next();
            });
          }
        ],
        done
      );
    });

    it('cleanup, should destroy the nexus without explicitly un-publishing it', (done) => {
      client.DestroyNexus({ uuid: UUID }, (err) => {
        if (err) return done(err);

        client.ListNexus({}, (err, res) => {
          if (err) return done(err);
          assert.lengthOf(res.nexus_list, 0);
          done();
        });
      });
    });
  }); // End of describe('nvmf')
});
