import pytest

from sprinkler.prompt_template import PromptTemplate
from sprinkler.runnable.task.prompt import PromptTask


def test_prompt_template_base():
    prompt = 'Jung Sik is {identity}'
    template = PromptTemplate(prompt)
    message = template.get_message(identity='genius')

    assert message['content'] == 'Jung Sik is genius'


def test_prompt_template_with_default():
    prompt = """
    Jung Sik is {identity}
    """
    template = PromptTemplate(prompt, 
                              input_config=
                              {'identity': {'type': str, 'default': 'genius'}})
    message = template.get_message()

    assert message['content'] == """
    Jung Sik is genius
    """


def test_prompt_template_with_type():
    prompt = 'Jung Sik is {identity}'
    template = PromptTemplate(prompt,
                              input_config={'identity': str})
    message = template.get_message(identity='genius')

    assert message['content'] == 'Jung Sik is genius'    


def test_prompt_template_invalid_type():
    prompt = 'Jung Sik is {identity}'
    template = PromptTemplate(prompt,
                              input_config={'identity': int})

    with pytest.raises(Exception) as err:
        message = template.get_message(identity='genius')

    assert 'validation' in str(err.value)  


def test_prompt_template_invalid_input():
    prompt = 'Jung Sik is {identity}'
    template = PromptTemplate(prompt)

    with pytest.raises(Exception) as err:
        message = template.get_message(gs='genius')

    assert 'identity' in err.value.args[0]


def test_prompt_task_base():
    messages = ['hello gpt!', PromptTemplate('Jungsik is {identity}')]

    task = PromptTask('prompt#1',
                      context={'messages': messages})
    
    output = task({'identity': 'genius'})

    assert output == [{'role': 'user', 'content': 'hello gpt!'},
                    {'role': 'user', 'content': 'Jungsik is genius'}]
    