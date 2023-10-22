OUTPUT_KEY = 'return'

DEFAULT_GROUP_INPUT_KEY = '__default__'

class _Null:
    def __bool__(self): return False

null = _Null()