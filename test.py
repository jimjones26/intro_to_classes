from typing import Any, Dict
import logging


# ------------------------------------------------------------------
# Component base class
# ------------------------------------------------------------------
class BaseComponent:
    """
    BaseComponent class for defining components with input and output dictionaries.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.inputs: Dict[str, Any] = {}
        self.outputs: Dict[str, Any] = {}

    def execute(self) -> None:
        """
        Logs the execution of the component and raises a NotImplementedError.
        Subclasses should override this method.
        """
        logging.info(f"Executing component: {self.name}")
        raise NotImplementedError(
            "The `execute` method is not implemented in this component. Please override this method in your subclass."
        )


# ------------------------------------------------------------------
# Pipeline class
# ------------------------------------------------------------------
class Pipeline:
    def __init__(self):
        self.components = []
        self.connections = {}

    def add_component(self, component: BaseComponent):
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
class TextPreprocessor(BaseComponent):
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
class TextLengthCalculator(BaseComponent):
    def __init__(self):
        super().__init__("Text Length Calculator")

    def execute(self):
        if "processed_text" in self.inputs:
            text_length = len(self.inputs["processed_text"])
            self.outputs = {"text_length": text_length}
            print(f"{self.name} output: {self.outputs}")


# ------------------------------------------------------------------
# Instantiate components and pipeline
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
final_output = pipeline.run(
    {"initial_input": "HelLo WorLD IS a GREat sonG about ThE EnD of theE WORLd."}
)
print("Final output:", final_output)
