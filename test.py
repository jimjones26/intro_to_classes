from typing import Any, Dict, List, Tuple
import logging

logging.basicConfig(level=logging.INFO)


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
        self.components: List[BaseComponent] = []
        self.connections: Dict[
            Tuple[BaseComponent, str], Tuple[BaseComponent, str]
        ] = {}

    def add_component(self, component: BaseComponent) -> None:
        self.components.append(component)

    def connect(
        self,
        output_component: BaseComponent,
        output_property: str,
        input_component: BaseComponent,
        input_property: str,
    ) -> None:
        # Store the connection using component instances and property names
        self.connections[(output_component, output_property)] = (
            input_component,
            input_property,
        )

    def run(self, initial_input: Dict[str, Any]) -> Dict[str, Any]:
        if not self.components:
            logging.error("No components in the pipeline.")
            return {}

        # Initialize the first component's input
        self.components[0].inputs.update(initial_input)

        for component in self.components:
            try:
                component.execute()
            except NotImplementedError as e:
                logging.error(f"Execution error in component {component.name}: {e}")
                continue

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

    @property
    def processed_text(self):
        return self.outputs["processed_text"]

    def execute(self):
        if self.inputs["initial_input"] is not None:
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
            input_keys=["input_text"],  # Keep this as is for flexibility
            output_keys=["text_length"],
        )

    @property
    def input_text(self):
        return self.inputs["input_text"]

    @property
    def text_length(self):
        return self.outputs["text_length"]

    def execute(self):
        if self.input_text is not None:
            text_length = len(self.input_text)
            self.outputs["text_length"] = text_length

            print(f"{self.name} output: {self.outputs}")


# ------------------------------------------------------------------
# Sample string int comparator component
# ------------------------------------------------------------------
class StringIntComparator(BaseComponent):
    def __init__(self):
        super().__init__(
            "String Int Comparator",
            input_keys=["input_string", "input_int"],
            output_keys=["output_string", "output_int"],
        )

    @property
    def input_string(self):
        return self.inputs["input_string"]

    @property
    def input_int(self):
        return self.inputs["input_int"]

    @property
    def output_string(self):
        return self.outputs["output_string"]

    @property
    def output_int(self):
        return self.outputs["output_int"]

    def execute(self):
        if self.input_string is not None and self.input_int is not None:
            if len(self.input_string) > self.input_int:
                self.outputs["output_string"] = self.input_string
                self.outputs["output_int"] = self.input_int
                print(f"{self.name} output: {self.outputs}")
            else:
                self.outputs["output_string"] = None
                self.outputs["output_int"] = None
                print(f"{self.name} output: Condition not met, outputs set to None")


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
pipeline.connect(process_text, "processed_text", calculate_length, "input_text")

# ------------------------------------------------------------------
# Run pipeline
# ------------------------------------------------------------------
final_output = pipeline.run(
    {"initial_input": "HelLo WorLD IS a GREat sonG about ThE EnD of thE WORLd."}
)
print("Final output:", final_output)
