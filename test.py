from typing import Any, Dict, List, Tuple
import logging


# ------------------------------------------------------------------
# Component base class
# ------------------------------------------------------------------
class BaseComponent:
    """
    BaseComponent class for defining components with dynamic input and output dictionaries.
    """

    def __init__(
        self, name: str, input_keys: List[str], output_keys: List[str]
    ) -> None:
        self.name = name
        self.inputs: Dict[str, Any] = {key: None for key in input_keys}
        self.outputs: Dict[str, Any] = {key: None for key in output_keys}

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
        self.connections: Dict[
            Tuple[BaseComponent, str], Tuple[BaseComponent, str]
        ] = {}

    def add_component(self, component: BaseComponent):
        self.components.append(component)

    def connect(
        self,
        output_component: BaseComponent,
        output_key: str,
        input_component: BaseComponent,
        input_key: str,
    ):
        if output_key not in output_component.outputs:
            raise ValueError(
                f"{output_component.name} does not have output key '{output_key}'"
            )
        if input_key not in input_component.inputs:
            raise ValueError(
                f"{input_component.name} does not have input key '{input_key}'"
            )

        self.connections[(output_component, output_key)] = (input_component, input_key)

    def run(self, initial_input: Dict[str, Any]) -> Dict[str, Any]:
        # Initialize the first component's input as a dictionary
        for component in self.components:
            if component.name == "Text Preprocessor":
                component.inputs.update(initial_input)

        for component in self.components:
            component.execute()
            # Transfer outputs to connected inputs
            for (output_component, output_key), (
                input_component,
                input_key,
            ) in self.connections.items():
                if component == output_component and output_key in component.outputs:
                    input_component.inputs[input_key] = component.outputs[output_key]

        # Assuming the last component's output is the final output
        return {component.name: component.outputs for component in self.components}


# ------------------------------------------------------------------
# Sample text preprocessor component
# ------------------------------------------------------------------
class TextPreprocessor(BaseComponent):
    def __init__(self):
        super().__init__(
            "Text Preprocessor",
            input_keys=["initial_input"],
            output_keys=["processed_text"],
        )

    def execute(self):
        if self.inputs["initial_input"] is not None:
            # Example preprocessing: convert to lowercase
            processed_text = self.inputs["initial_input"].lower()
            self.outputs["processed_text"] = processed_text
            print(f"{self.name} output: {self.outputs}")


# ------------------------------------------------------------------
# Sample text length caluculator component
# ------------------------------------------------------------------
class TextLengthCalculator(BaseComponent):
    def __init__(self):
        super().__init__(
            "Text Length Calculator",
            input_keys=["processed_text"],
            output_keys=["text_length"],
        )

    def execute(self):
        if self.inputs["processed_text"] is not None:
            text_length = len(self.inputs["processed_text"])
            self.outputs["text_length"] = text_length
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
    {"initial_input": "HelLo WorLD IS a GREat sonG about ThE EnD of thE WORLd."}
)
print("Final output:", final_output)
