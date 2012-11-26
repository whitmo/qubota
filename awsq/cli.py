from boto.sqs.message import RawMessage
from botox import aws
from cliff.app import App
from cliff.command import Command
from cliff.commandmanager import CommandManager
import pkg_resources


class CLIApp(App):
    """
    command line interface
    """
    specifier = 'awsq.cli'
    version = pkg_resources.get_distribution('awsq').version
    prefix = 'awsq'

    def __init__(self):
        self._settings = None
        self._aws = None
        super(CLIApp, self).__init__(
            description=self.specifier,
            version=self.version,
            command_manager=CommandManager(self.specifier)
        )

    @property
    def aws(self):
        if self._aws is None:
            #@ parameterize
            # we assume use of environ vars
            self._aws = aws.AWS()
        return self._aws

    def qinsts(self):
        return (x for x in self.aws.instances() if x.name.startswith(self.prefix))


class QUp(Command):
    """
    Brings up or builds a queue system

    - launch and/or bootstrap precise cloudinit instances
    """
    def get_parser(self, prog_name):
        parser = super(QUp, self).get_parser(prog_name)
        parser.add_argument('--queue', '-q', default=CLIApp.prefix, help="Queue name")
        return parser

    def take_action(self, pargs):
        """
        check, ammend, return
        """
        if not len(self.app.qinsts):
            # up
            pass
        import pdb;pdb.set_trace()
        print([x for x in self.app.qinsts])
        


class QDown(Command):
    """
    Brings the queue system down

    - shutdown ec2 instances
    - other ??
    """
    def take_action(self, pargs):
        for inst in self.app.qinsts:
            inst.terminate()
            print("%s terminated" %inst.name)


class EnqueueJob(Command):
    
    def take_action(self, pargs):
        """
        enqueue a job
        """
        pass


class WorkerDaemon(Command):
    """
    Runs a worker daemon
    """
    def get_parser(self, prog_name):
        parser = super(WorkerDaemon, self).get_parser(prog_name)
        parser.add_argument('--queue', '-q', default=CLIApp.prefix, help="Queue name")
        return parser
    
    def take_action(self, pargs):
        """
        launch daemon
        """
        pass


