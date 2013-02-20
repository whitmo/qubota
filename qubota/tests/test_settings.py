def test_settings_decorator():
    from qubota.service import Setting
    
    @Setting.initialize_all
    class Hoopty(object):
        monkey = Setting(default=0, help='help')

    settings = getattr(Hoopty, '_defaults', None)
    assert settings.monkey
    assert settings.monkey.default == 0
    assert settings.monkey.help == 'help'
        
    
def test_service_config():
    from ..drain import Drain
    assert 'poll_interval' in  Drain._defaults
