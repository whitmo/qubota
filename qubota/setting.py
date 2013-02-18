from stuf import frozenstuf
from stuf import stuf


class Config(stuf):
    """
    A data structure for configuration information
    """
    load = stuf.update


class SettingInfo(frozenstuf):
    def to_defaults(self):
        return dict((key, value.default) for key, value in self.items())


def match_descriptors(klass, descriptor_class):
    for name, inst in klass.__dict__.items():
        if isinstance(inst, descriptor_class):
            yield name, inst


class Setting(object):
    def __init__(self, default=None, help=None):
        self.default = default
        self.help = help
        self.name = None

    def set_name(self, name):
        self.name = name

    def __get__(self, obj, objtype=None):
        if self.name is None:
            raise RuntimeError("name not initialized")
        return obj.config[self.name]
    
    def __set__(self, obj, value):
        raise ValueError("Read only value")

    @classmethod
    def initialize_all(cls, klass, extractor=match_descriptors, info_ctor=SettingInfo, 
                       attr='_defaults'):
        settings  = {name: setting for name, setting in extractor(klass, cls)}

        # annotate the klass
        setattr(klass, attr, info_ctor(settings)) 

        # set the name for each of the descriptors
        # since this is the first time we know it
        for name, setting in settings.items():
            setting.set_name(name)
        return klass
