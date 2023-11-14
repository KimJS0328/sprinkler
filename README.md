# 🌱 Sprinkler 🌱

![Version](https://img.shields.io/badge/version-0.1.0-Green)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Open in Dev Containers](https://img.shields.io/static/v1?label=Dev%20Containers&message=Open&color=blue&logo=visualstudiocode)

### 🛠️ The Tool for constructing pipeline for your application

Origin from graudation project from Sungkyunkwan University

## ☄️ How to install (TBD)

Install easily with [`PyPI - Sprinkler`](pypi-sprinkler) !!!

```
pip install sprinkler
``````

## 🦷 Requirements

Please install below packages for Sprinkler. We recommed using `pip` command as sprinkler.

* python 3.8+
* pydantic 2.4.2+
* openapi 0.28.0 (optional - for chat completion of GPT)
* pygraphviz 1.11 (optional - for visualization of pipeline)

If you want to visualize your own pipeline, after install pygraphviz packages,  you sholuld install `Graphviz` and `C/C++ Compiler` depeding on your OS. For `Graphviz` with Linux Ubuntu, installation command is below.

```bash
sudo apt-get install graphviz graphviz-dev
```

If you need some installation guide of Graphviz on different OS, The instruction manual is [here](https://github.com/pygraphviz/pygraphviz/blob/main/INSTALL.txt)

## 😆 What is Sprinkler for?

As apperance of ChatGPT, LLM (Large Language Model) becomes the talk of all programming parts. As active research on LLM is ongoing, there are many efforts to utilize LLM to application for user. Although LLM is so powerful itself, the limit is also clear. So, we should divide the whole task we want solve utilizing LLM into smaller tasks and constructe them into pipeline sequentialy or in parallel. 

Sprinkler is developed for this purposes. From Sprinkler, you can imporve reusability  and stability. Sprinkler has main three components which are runnable.

* **Task**  - the runnalbe which operates one function
* **Pipeline** - the runnable which operates runnables sequentially
* **Group** - the runnable which operates runnables in parallel

You  can make your own pipeline system with these components. For more information, Let's see next section for usage.

## 👀 Examples

The one basic examples of just repeating something using `Task`, `Pipeline`, `Group` is like below.

```python
def repeat_string(string: str, repeat: int = 3) -> str:
    return string * repeat
def repeat_array(array: list, repeat: int = 3) -> list:
    return array * repeat

def group_with_processpool():

    pipeline1 = Pipeline('pipeline1')
    pipeline1.add(Task('repeat_string', repeat_string))

    pipeline2 = Pipeline('pipeline2')
    pipeline2.add(Task('repeat_array', repeat_array))

    group = Group('group').add(
        pipeline1, pipeline2
    )

    with ProcessPoolExecutor(2) as executor:
        output = group.run(
            pipeline1=('sprinkler',),
            pipeline2=([1,2,3],),
            __executor__=executor
        )

    # output becomes 
    # {
    #   'pipeline1': 'sprinklersprinklersprinkler',
    #   'pipeline2': [1, 2, 3, 1, 2, 3, 1, 2, 3]
    # }
```

With GPT Chatcompletion, you can construct your pipeline like below.

```python
def pipeline_prompt_chat():
    messages = [SystemPromptTemplate('You are a fan of baseball team named {team}'),
                PromptTemplate('Have you bought {team} merchandise?')]

    task_prompt = PromptTask('prompt',{'messages': messages})
    task_chat = ChatCompletionTask('chat')
    
    pipeline = Pipeline('pipeline').add(task_prompt, task_chat)

    output = pipeline.run({'team': 'sprinkler'})

    # output becomes like (example)
    # Yes, as a fan of the Sprinkler baseball team, I have bought various sprinkler merchandise. This includes hats, jerseys, t-shirts, keychains, and even a mini sprinkler for my garden. Showcasing my support for the team and representing them through merchandise is a fun way to connect with fellow fans and show my love for the Sprinkler baseball team
```

## 🧑🏻‍💻 Contributing

Open Source projects and cultures are actively developing in this era. Our project is also open to every contributors and feedback from users. 