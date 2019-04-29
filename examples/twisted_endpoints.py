"""
This example makes use of the Twisted endpoints API.

This example can be run with ``twistd -n -y examples/twisted_endpoints.py``.

For general information on endpoints, see the endpoints overview at
https://twistedmatrix.com/documents/current/core/howto/endpoints.html.
"""
from twisted.application import service, internet
from twisted.internet import defer, endpoints, protocol, reactor
from twisted.logger import Logger
from pika import exceptions
from pika.adapters.twisted_connection import TwistedProtocolConnection

_log = Logger(__name__)

# The following is boilerplate to construct a Twisted application to hold the
# AMQP client service object.
#
# In a normal application, the string provided to the
# ``endpoints.clientFromString`` call would be a configurable value.
application = service.Application("Demo AMQP Client")
amqp_endpoint = endpoints.clientFromString(reactor, 'tcp:localhost:5672')
amqp_factory = protocol.Factory.forProtocol(TwistedProtocolConnection)
amqp_service = internet.ClientService(amqp_endpoint, amqp_factory)
amqp_service.setServiceParent(application)


@defer.inlineCallbacks
def on_connected(connection):
    """
    Callback invoked when a connection has been established by Twisted.

    Args:
        connection (TwistedProtocolConnection): The active protocol.
    """
    # Wait for Pika to mark the connection as ready.
    yield connection.ready

    try:
        channel = yield connection.channel()
        yield channel.queue_declare("twisted_queue")
        yield channel.basic_publish("", "twisted_queue", "Hello Twisted")
        _, frame, properties, body = yield channel.basic_get(
            queue="twisted_queue")
        _log.info(body)
        yield channel.basic_ack(delivery_tag=frame.delivery_tag)
    except exceptions.AMQPError as e:
        # Any of these calls can result in an exception. These may be due to
        # permission errors, an interrupted connection, lack of available
        # channels, and so on. Fine-grain exception classes are available for
        # each case.
        _log.critical("An unhandled error occurred: {e}", e=e)

    reactor.stop()


def on_connected_failed(failure):
    """
    Callback invoked when a connection can't be established by Twisted

    Args:
        failure: The reason the connection isn't available.
    """
    if failure.check(defer.CancelledError):
        _log.info("Service has halted.")
        reactor.stop()
    else:
        # In this case the failAfterFailures limit was reached. It could be
        # DNSLookupError or ConnectionRefusedError.
        _log.error("Connection failed: {}".format(str(failure)))
        reactor.stop()


# Retrieve a Deferred that will fire with the currently-connected
# TwistedProtocolConnection. The optional ``failAfterFailures`` argument allows
# us to specify how many times the connection can fail to be established before
# calling the error callback; by default it will try as long as the service is
# running. You can use this API wherever you need an AMQP client connection.
deferred_conn = amqp_service.whenConnected(failAfterFailures=3)
deferred_conn.addCallbacks(on_connected, on_connected_failed)
