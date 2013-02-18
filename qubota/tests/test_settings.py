

def test_settings_decorator():
    from qubota.setting import Setting
    
    @Setting.initialize_all
    class Hoopty(object):
        monkey = Setting(default=0, help='help')

    settings = getattr(Hoopty, '_defaults', None)
    assert settings.monkey
    assert settings.monkey.default == 0
    assert settings.monkey.help == 'help'
        
    
def test_service_config():
    from ..service import Drain
    assert 'poll_interval' in  Drain._defaults
