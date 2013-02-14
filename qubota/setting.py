from stuf import frozenstuf

class Setting(object):
    def __init__(self, default=None, help=None):
        self.default = default
        self.help = help

def settings_for_class(klass):
    for attr in dir(klass):
        if attr.startswith('__'):
            continue
        candidate = getattr(klass, attr)
        if isinstance(candidate, Setting):
            yield attr, candidate

def collect_setting_info(klass, extractor=settings_for_class):
    settings  = {name: {'help': setting.help, 
                        'default': setting.default} \
                     for name, setting in extractor(klass)}
    klass._settings = frozenstuf(settings)
    return klass
