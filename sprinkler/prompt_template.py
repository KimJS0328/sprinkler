from __future__ import annotations

from typing import Any, Dict

from pydantic import create_model, ValidationError


class PromptTemplate:
    """Class for template of prompt"""

    prompt: str


    def __init__(
        self, 
        prompt: str,
        *, 
        input_config: Dict[str, Any | Dict[str, Any]] | None = None
    ) -> None:
        """Save prompt and create pydantic model for validation (optional)

        Args:
            prompt: format string for prompt
            input_config(optional): dictionary for refine type and default
            value for input variable in prompt.
        """
        # Save given prompt in instance
        self.prompt = prompt

        if input_config is not None:
            _input_config = {}
            # A dictionary for create pydantic model
            # Key: input variable name
            # Value: dictionary with 'type' and 'default' of the variable

            for input_name, config in input_config.items():
                if not (('{' + input_name + '}') in prompt):
                    raise Exception(f'{input_name} is not in the given prompt.')
                
                _input_config[input_name] = {}
                if isinstance(config, dict):
                    _input_config[input_name]['type'] = config.get('type') or str
                    _input_config[input_name]['default'] = config.get('default')
                else:
                    _input_config[input_name] = {'type': config}

            # create input pydantic model for validation
            self._input_model = create_model(
                'PromptInput',
                **{
                    name: (config['type'], config.get('default') or ...)
                    for name, config in _input_config.items()
                }
            )        


    def _validate_input(self, **kwargs):
        """Validate input variables from prompt"""  
        if '_input_model' in self.__dict__:
            try:
                return  (self._input_model
                    .model_validate(kwargs)
                    .model_dump())
        
            except ValidationError as e:
                raise Exception(f'PromptTemplate input: {e}')
        else:
            return kwargs
        
    
    def get_message(self, **kwargs) -> str:
        """Format the prompt with the inputs.

        Args:
            kwargs: Any arguments to be passed to the prompt template.

        Returns:
            A formatted string.
        """

        if '_input_model' in self.__dict__:
            kwargs = self._validate_input(**kwargs)

        return {'role': 'user', 'content': self.prompt.format(**kwargs)}


class SystemPromptTemplate(PromptTemplate):
    """Class for a template of prompt as system"""

    def get_message(self, **kwargs) -> str:
        if '_input_model' in self.__dict__:
            kwargs = self._validate_input(**kwargs)

        return {'role': 'system', 'content': self.prompt.format(**kwargs)}

class AssistantPromptTemplate(PromptTemplate):
    """Class for a template of prompt as assistant"""

    def get_message(self, **kwargs) -> str:
        if '_input_model' in self.__dict__:
            kwargs = self._validate_input(**kwargs)

        return {'role': 'assistant', 'content': self.prompt.format(**kwargs)}