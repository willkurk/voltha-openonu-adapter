from twisted.internet import reactor, task
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.python import reflect
from twisted.protocols import amp
from ampoule import pool, util, child
from pyvoltha.adapters.kafka.adapter_proxy import AdapterProxy
from pyvoltha.adapters.kafka.core_proxy import CoreProxy
from pyvoltha.adapters.extensions.omci.openomci_agent import OpenOMCIAgent, OpenOmciAgentDefaults
from pyvoltha.adapters.extensions.omci.omci_me import *
from pyvoltha.adapters.extensions.omci.database.mib_db_ext import MibDbExternal
from pyvoltha.common.utils.registry import registry, IComponent
from zope.interface import implementer
from pyvoltha.adapters.kafka.adapter_request_facade import InterAdapterRequestFacade
from pyvoltha.adapters.kafka.kafka_inter_container_library import IKafkaMessagingProxy, \
    get_messaging_proxy
import time
from copy import deepcopy
import sys
import os
sys.path.insert(0, "/voltha/")

from brcm_openomci_onu_handler import BrcmOpenomciOnuHandler
from omci.brcm_capabilities_task import BrcmCapabilitiesTask
from omci.brcm_mib_sync import BrcmMibSynchronizer

import dill

from voltha_protos.device_pb2 import Device
from voltha_protos.inter_container_pb2 import InterContainerMessage

class OMCIAdapter:
    def __init__(self, process_parameters, broadcom_omci):
        self.process_parameters = process_parameters
        self.broadcom_omci = broadcom_omci

class BrcmAdapterShim:
    def __init__(self, core_proxy, adapter_proxy, omci_agent, broadcom_omci, event_bus):
        self.core_proxy = core_proxy
        self.adapter_proxy = adapter_proxy
        self.omci_agent = omci_agent
        self.broadcom_omci = broadcom_omci
        self.event_bus = event_bus

    def custom_me_entities(self):
        return None

    def start(self):
        log.debug('starting')
        self.omci_agent.start()
        log.info('started')

    def mibSyncComplete(self, device):
        log.debug("mib-sync-complete-sending")
        self.event_bus.mibSyncComplete(device)
        
@implementer(IComponent)
class MainShim:
    def __init__(self, args):
        self.args = args
    def get_args(self):
        return self.args

class ProcessMessage(amp.Argument):
        def toString(self, inObject):
            return inObject.SerializeToString()

        def fromString(self, inString):
            message = InterContainerMessage()
            message.ParseFromString(inString())
            return message

class OMCIDeviceParam(amp.Argument):
        def toString(self, inObject):
            #log.debug("serializing-device")
            return inObject.SerializeToString()

        def fromString(self, inString):
            device = Device()
            device.ParseFromString(inString)
            return device

class OMCIAdapterParam(amp.Argument):
        def toString(self, inObject):
            #log.debug("serializing-device")
            return dill.dumps(inObject)

        def fromString(self, inString):
            return dill.loads(inString)

class ProcessMessage(amp.Command):
    arguments = [('message', ProcessMessage())]
    response = [("success", amp.Integer())]

class Activate(amp.Command):
    arguments = [("device", OMCIDeviceParam()),
            ("adapter", OMCIAdapterParam())]
    response = [("success", amp.Integer())]

class OMCIDevice(child.AMPChild):
    handler = {}
    activated = {}
    init_state = "init"
    max_devices = 8
    device_queue = []
    devices_processing = {}
    inter_adapter_message_queue = {}
    @Activate.responder
    def activate(self, device, adapter):
        import structlog
        #from adapters.brcm_openomci_onu.brcm_openomci_onu import *
        #from twisted.python import log as logtwisted
        #logtwisted.startLogging(sys.stdout)
        
        self.log = structlog.get_logger()
        self.log.info("entering-activate-process")
        self.activated[device.id] = False
         
        if len(self.devices_processing.keys()) >= self.max_devices:
            self.device_queue.append((device,adapter))
        else:
            reactor.callLater(0, self.initAndActivate, device, adapter)
            self.devices_processing[device.id] = device
            #self.initAndActivate(device, adapter)
        return {"success": 1}

    @inlineCallbacks
    def initAndActivate(self, device, adapter):
        self.log = structlog.get_logger()
        #from adapters.brcm_openomci_onu.brcm_openomci_onu import *
        #from twisted.python import log as logtwisted
        #logtwisted.startLogging(sys.stdout)
        #import os
        #ptvsd.enable_attach(address=('10.64.1.131', 3000+os.getpid()), redirect_output=True)
        #self.log.debug("debugger port :" + str(3000+os.getpid()))
        #ptvsd.wait_for_attach()

        try: 
            if self.init_state == "init":
                self.init_state = "initializing"
                self.log.debug("initializing-handler")
                
                main_shim = MainShim(adapter.process_parameters["args"])
                registry.register('main', main_shim)
        
                self.broadcom_omci = deepcopy(OpenOmciAgentDefaults)
            
                self.broadcom_omci['mib-synchronizer']['state-machine'] = BrcmMibSynchronizer
                self.broadcom_omci['mib-synchronizer']['database'] = MibDbExternal
                self.broadcom_omci['omci-capabilities']['tasks']['get-capabilities'] = BrcmCapabilitiesTask 

                self.core_proxy = CoreProxy(
                    kafka_proxy=None,
                    default_core_topic=adapter.process_parameters["core_topic"],
                    default_event_topic=adapter.process_parameters["event_topic"],
                    my_listening_topic=adapter.process_parameters["listening_topic"])

                self.adapter_proxy = AdapterProxy(
                    kafka_proxy=None,
                    core_topic=adapter.process_parameters["core_topic"],
                    my_listening_topic=adapter.process_parameters["listening_topic"])

                openonu_request_handler = InterAdapterRequestFacade(adapter=self,
                                                               core_proxy=self.core_proxy)
                self.log.debug("starting-kafka-client")
            #self.log.debug(os.getpid()+ " pid")
            #self.log.debug(adapter.process_parameters["args"].instance_id)
                yield registry.register(
                    'kafka_adapter_proxy',
                    IKafkaMessagingProxy(
                        kafka_host_port=adapter.process_parameters["args"].kafka_adapter,
                        # TODO: Add KV Store object reference
                        kv_store=adapter.process_parameters["args"].backend,
                        default_topic=adapter.process_parameters["args"].name,
                        group_id_prefix=adapter.process_parameters["args"].instance_id+str(os.getpid()),
                        target_cls=openonu_request_handler,
                        manual_offset=long(adapter.process_parameters["offset"])
                    )
                ).start()

                self.core_proxy.kafka_proxy = get_messaging_proxy()
                self.adapter_proxy.kafka_proxy = get_messaging_proxy()

                self.log.debug("initializing-omci-agent")
                self.omci_agent = OpenOMCIAgent(self.core_proxy,
                                                 self.adapter_proxy,
                                                     support_classes=self.broadcom_omci)
                self.adapterShim = BrcmAdapterShim(self.core_proxy, self.adapter_proxy, self.omci_agent,self.broadcom_omci, self) 
                self.adapterShim.start()

                self.init_state = "done"
            elif self.init_state == "initializing":
                self.log.debug("initialization-in-progress")
                reactor.callLater(1, self.initAndActivate, device, adapter)
                return

            self.handler[device.id] = BrcmOpenomciOnuHandler(self.adapterShim, device.id)
            yield self.handler[device.id].activate(device)
            self.log.debug("finished-activating", device_id=device.id)
            self.activated[device.id] = True

            
            if device.id in self.inter_adapter_message_queue.keys():
                log.debug("popping-queued-inter-adapter-messages", size=len(self.inter_adapter_message_queue[device.id]), device_id=device.id)
                while len(self.inter_adapter_message_queue[device.id]) > 0:
                    msg = self.inter_adapter_message_queue[device.id].pop()
                    if msg.header:
                        if msg.header.to_device_id in self.handler.keys():
                            self.handler[msg.header.to_device_id].process_inter_adapter_message(msg)
            else:
                log.debug("No message queued", device_id=device.id)
        except Exception as err:
            self.log.error("Exception:", err=err)
#    @ProcessMessage.responder
#   def processMessage(self, msg):
#        import os
#        import structlog
#        self.log.debug("sending-process-message")
#        self.handler.process_inter_adapter_message(msg)
#        millis = int(round(time.time() * 1000))
#        self.log.debug("started-process-message", millis=millis)
#        return {"success": 1}
    def get_ofp_port_info(self, device, port_no):
        try:
            if device.id in self.handler.keys():
                ofp_port_info = self.handler[device.id].get_ofp_port_info(device, port_no)
                log.debug('get_ofp_port_info', device_id=device.id, ofp_port_info=ofp_port_info)
                return ofp_port_info, True
            else:
                return None, False
        except Exception as err:
            self.log.error("Exception:", err=err)

    def process_inter_adapter_message(self, msg):
        try:
            self.log = structlog.get_logger()
            self.log.debug('process-inter-adapter-message', device_id=msg.header.to_device_id, msg=msg)
            # Unpack the header to know which device needs to handle this message
            if msg.header:
                if msg.header.to_device_id in self.activated.keys():
                    if not self.activated[msg.header.to_device_id]:
                        self.log.debug("process-inter-adapter-message-queue-until-activated", device_id=msg.header.to_device_id)
                        #reactor.callLater(5, self.process_inter_adapter_message, msg)
                        if not msg.header.to_device_id in self.inter_adapter_message_queue.keys():
                            self.inter_adapter_message_queue[msg.header.to_device_id] = []
                        self.inter_adapter_message_queue[msg.header.to_device_id].append(msg)
                        return None, False 
            if msg.header:
                if msg.header.to_device_id in self.handler.keys():
                    self.handler[msg.header.to_device_id].process_inter_adapter_message(msg)
                    return None, True
            return None, False
        except Exception as err:
            self.log.error("Exception:", err=err)

    def update_flows_bulk(self, device, flows, groups):
        '''
        log.info('bulk-flow-update', device_id=device.id,
                  flows=flows, groups=groups)
        '''
        try:
            assert len(groups.items) == 0
            if device.id in self.handler.keys():
                return self.handler[device.id].update_flow_table(device, flows.items), True
            else:
                return None, False
        except Exception as err:
            self.log.error("Exception:", err=err)

    def update_flows_incrementally(self, device, flow_changes, group_changes):
        raise NotImplementedError()

    def mibSyncComplete(self, doneDevice):
        self.log.debug("checking-processing-devices", doneDevice=doneDevice)
        if doneDevice.id in self.devices_processing.keys():
            self.log.debug("in list")
            del self.devices_processing[doneDevice.id]
            if len(self.devices_processing.keys()) < self.max_devices:
                (device, adapter) = self.device_queue.pop(0)
                self.log.debug("new device", device=device)
                reactor.callLater(0, self.initAndActivate, device, adapter)
                self.devices_processing[device.id] = device
                self.log.debug("called activate")

