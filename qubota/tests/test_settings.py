

def test_settings_decorator():
    from qubota.setting import collect_setting_info
    from qubota.setting import Setting
    
    @collect_setting_info
    class Hoopty(object):
        monkey = Setting(default=0, help='help')

    settings = getattr(Hoopty, '_settings', None)
    assert settings.monkey
    assert settings.monkey.default == 0
    assert settings.monkey.help == 'help'
        
    
def test_service_config():
    from ..service import Drain
    assert 'poll_interval' in  Drain._settings
