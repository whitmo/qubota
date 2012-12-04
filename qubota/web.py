from ginkgo import Service
from ginkgo import Setting
from ginkgo.async.gevent import _ServerWrapper
from socketio.server import SocketIOServer as BaseServer
from pyramid.config import Configurator
from socketio.namespace import BaseNamespace
from socketio import socketio_manage
from socketio.mixins import BroadcastMixin


class WorkerWeb(Service):
    port = Setting('port', 
                   default=7337,
                   help="How often to wake up and check the workers")
    hostname = Setting('hostname', 
                       default='0.0.0.0',
                       help="How often to wake up and check the workers")

    def __init__(self):
        self.add_service(SocketIOServer((self.hostname, self.port),
                                        self.app(), resource='socket.io', 
                                        policy_server=True, policy_listener=(self.hostname, 10843)))

    def app(self):
        config = Configurator()
        simple_route(config, 'index', '/', lambda req: {})
        # The socketio view configuration
        simple_route(config, 'socket_io', 'socket.io/*remaining', socketio_service)
        config.add_static_view('static', 'web', cache_max_age=3600)
        app = config.make_wsgi_app()
        return app        


def simple_route(config, name, url, fn):
    """
    Function to simplify creating routes in pyramid
    Takes the pyramid configuration, name of the route, url, and view
    function.
    """
    config.add_route(name, url)
    config.add_view(fn, route_name=name,
            renderer="qubota:web/%s.html" % name)


class SocketIOServer(_ServerWrapper):
    server = BaseServer

#@ c&p to be modded

class NamedUsersRoomsMixin(BroadcastMixin):
    def __init__(self, *args, **kwargs):
        super(NamedUsersRoomsMixin, self).__init__(*args, **kwargs)
        if 'rooms' not in self.session:
            self.session['rooms'] = set()  # a set of simple strings
            self.session['nickname'] = 'guest123'

    def join(self, room):
        """Lets a user join a room on a specific Namespace."""
        self.socket.rooms.add(self._get_room_name(room))

    def leave(self, room):
        """Lets a user leave a room on a specific Namespace."""
        self.socket.rooms.remove(self._get_room_name(room))

    def _get_room_name(self, room):
        return self.ns_name + '_' + room

    def emit_to_room(self, event, args, room):
        """This is sent to all in the room (in this particular Namespace)"""
        pkt = dict(type="event",
                   name=event,
                   args=args,
                   endpoint=self.ns_name)
        room_name = self._get_room_name(room)
        for sessid, socket in self.socket.server.sockets.iteritems():
            if not hasattr(socket, 'rooms'):
                continue
            if room_name in socket.rooms:
                socket.send_packet(pkt)


class ChatNamespace(BaseNamespace, NamedUsersRoomsMixin):
    def on_chat(self, msg):
        self.broadcast_event('chat', msg)

    def recv_connect(self):
        self.broadcast_event('user_connect')

    def recv_disconnect(self):
        self.broadcast_event('user_disconnect')
        self.disconnect(silent=True)

    def on_join(self, channel):
        self.join(channel)



from pyramid.response import Response
def socketio_service(request):
    socketio_manage(request.environ,
                    {'/chat': ChatNamespace},
                    request=request)

    return Response('')
