import struct
import gevent


class Brot(object):
    color_max = 255

    M ='<QIIHHHH'

    def __init__(self, v=1500, x=1000, nb=False):
        self.v, self.x = v, x
        self.C = range(v*x)
        self.nb = nb

    def func1(self, V, B, c):
        if self.nb:
            gevent.sleep(0)
        return c and self.func1(V*V+B, B, c-1) \
            if abs(V) < 6 else (2+c-4*abs(V)**-0.4)/self.color_max

    def func2(self, T):
        return T*80+T**9*self.color_max-950*T**99, T*70-880*T**18+701*T**9, T*self.color_max**(1-T**45*2)

    def render(self):
        v = self.v
        x = self.x 
        yield 'BM' + struct.pack(self.M, v*x*3+26, 26, 12, v, x, 1, 24)
        for X in self.C:
            summed = sum(self.func1(0, (A % 3/3.0+X % v+(X/v+A/3/3.0-x/2)/1j) \
                                        * 2.5/x-2.7,
                                    self.color_max)**2 for A in self.C[:9])
            args = self.func2(summed / 9)
            iteration = struct.pack('BBB', *args)
            yield iteration

    def to_file(self, outfile):
        with open(outfile,'wb') as out:
            for data in self.render():
                out.write(data)

    def to_file_lb(self, outfile):
        with open(outfile,'wb') as out:
            for data in self.render():
                gevent.sleep(0)
                out.write(data)
