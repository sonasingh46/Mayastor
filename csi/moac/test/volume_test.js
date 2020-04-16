// Unit tests for the volume object
//
// The tests for more complex volume methods are in volumes_test.js mainly
// because volumes.js takes care of routing registry events to the volume
// and it makes sense to test this together.

'use strict';

const expect = require('chai').expect;
const sinon = require('sinon');
const Nexus = require('../nexus');
const Node = require('../node');
const Pool = require('../pool');
const Registry = require('../registry');
const Replica = require('../replica');
const Volume = require('../volume');

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';

const defaultOpts = {
  replicaCount: 1,
  preferredNodes: [],
  requiredNodes: [],
  requiredBytes: 100,
  limitBytes: 100,
};

module.exports = function () {
  it('should stringify volume name', () => {
    let registry = new Registry();
    let volume = new Volume(UUID, registry, defaultOpts);
    expect(volume.toString()).to.equal(UUID);
  });

  it('should get name of the node where the volume is accessible from', () => {
    let registry = new Registry();
    let volume = new Volume(UUID, registry, defaultOpts);
    let node = new Node('node');
    let nexus = new Nexus({ uuid: UUID });
    nexus.bind(node);
    volume.newNexus(nexus);
    expect(volume.getNodeName()).to.equal('node');
  });

  it('should get zero size of a volume that has not been created yet', () => {
    let registry = new Registry();
    let volume = new Volume(UUID, registry, defaultOpts);
    expect(volume.getSize()).to.equal(0);
  });

  it('should set the preferred nodes for the volume', () => {
    let registry = new Registry();
    let volume = new Volume(UUID, registry, defaultOpts);
    expect(volume.preferredNodes).to.have.lengthOf(0);
    let updated = volume.update({ preferredNodes: ['node1', 'node2'] });
    expect(updated).to.equal(true);
    expect(volume.preferredNodes).to.have.lengthOf(2);
  });

  it('should publish and unpublish the volume', async () => {
    let registry = new Registry();
    let volume = new Volume(UUID, registry, defaultOpts);
    let node = new Node('node');
    let nexus = new Nexus({ uuid: UUID });
    let stub = sinon.stub(node, 'call');
    nexus.bind(node);
    volume.newNexus(nexus);

    stub.resolves({ devicePath: '/dev/nbd0' });
    await volume.publish('nbd');
    expect(nexus.devicePath).to.equal('/dev/nbd0');
    sinon.assert.calledOnce(stub);
    sinon.assert.calledWithMatch(stub, 'publishNexus', {
      uuid: UUID,
      key: '',
    });

    stub.resolves({});
    await volume.unpublish();
    expect(nexus.devicePath).to.equal('');
    sinon.assert.calledTwice(stub);
    sinon.assert.calledWithMatch(stub.secondCall, 'unpublishNexus', {
      uuid: UUID,
    });
  });
};
