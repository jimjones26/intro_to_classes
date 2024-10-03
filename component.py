class Component:
    def __init__(self, name):
        self.name = name

    def execute(self, *args):
        raise NotImplementedError("Each component must implement the execute method.")
