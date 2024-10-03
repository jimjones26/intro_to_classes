# ------------------------------------------------------------------
# Component base class
# ------------------------------------------------------------------
class Component:
    def __init__(self, name):
        self.name = name
        self.inputs = {}
        self.outputs = {}

    def execute(self):
        raise NotImplementedError("Each component must implement the execute method.")


# ------------------------------------------------------------------
# Pipeline class
# ------------------------------------------------------------------
class Pipeline:
    def __init__(self):
        self.components = []
        self.connections = {}

    def add_component(self, component: Component):
        self.components.append(component)

    def connect(self, output_component, output_key, input_component, input_key):
        self.connections[(output_component, output_key)] = (input_component, input_key)

    def run(self, initial_input):
        # Initialize the first component's input as a dictionary
        self.components[0].inputs = initial_input

        for component in self.components:
            component.execute()
            # Transfer outputs to connected inputs
            for (output_component, output_key), (
                input_component,
                input_key,
            ) in self.connections.items():
                if component == output_component:
                    input_component.inputs[input_key] = component.outputs[output_key]

        # Assuming the last component's output is the final output
        return self.components[-1].outputs


# ------------------------------------------------------------------
# Sample text preprocessor component
# ------------------------------------------------------------------
class TextPreprocessor(Component):
    def __init__(self):
        super().__init__("Text Preprocessor")

    def execute(self):
        if "initial_input" in self.inputs:
            # Example preprocessing: convert to lowercase
            processed_text = self.inputs["initial_input"].lower()
            self.outputs = {"processed_text": processed_text}
            print(f"{self.name} output: {self.outputs}")


# ------------------------------------------------------------------
# Sample text length caluculator component
# ------------------------------------------------------------------
class TextLengthCalculator(Component):
    def __init__(self):
        super().__init__("Text Length Calculator")

    def execute(self):
        if "processed_text" in self.inputs:
            text_length = len(self.inputs["processed_text"])
            self.outputs = {"text_length": text_length}
            print(f"{self.name} output: {self.outputs}")


# ------------------------------------------------------------------
# Instantiate component and pipeline
# ------------------------------------------------------------------
process_text = TextPreprocessor()
calculate_length = TextLengthCalculator()

pipeline = Pipeline()

# ------------------------------------------------------------------
# Add component to pipeline
# ------------------------------------------------------------------
pipeline.add_component(process_text)
pipeline.add_component(calculate_length)

# ------------------------------------------------------------------
# Connect components
# ------------------------------------------------------------------
pipeline.connect(process_text, "processed_text", calculate_length, "processed_text")

# ------------------------------------------------------------------
# Run pipeline
# ------------------------------------------------------------------
final_output = pipeline.run({"initial_input": "HelLo WorLD"})
print("Final output:", final_output)
