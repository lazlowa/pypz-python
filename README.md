# Description
*pypz* (pipes [paɪps]) is a lightweight and modular Pipeline as Code (PaC) framework.
Its purpose is to simplify and speed up the process of developing **data processing**
pipelines by taking care of most of the challenges and repetitive tasks, so you
can focus on the development of the business logic.

# Disclaimer
*pypz* is (was?) a one-man free-/part-time activity.

Originally, *pypz* has been designed to address some data processing related challenges in
both home and work environment, like:
- how could we speed up and modularize the development of such data processing tasks
- how could we easily perform data processing on huge scale
- how could we speed up the data processing itself
- 
# *Why should I use it?*

Inspired by many different opensource workflow/pipeline frameworks, *pypz* has to
following advantages:
- No dedicated executor infrastructure is necessary, since pypz operators are self-contained
i.e., the executor is integrated
- The entire development workflow can remain in code context from development of the operators
until the design and deployment of the pipelines 
- Natively supports data transfer between operators even in streaming mode
- Natively supports deployment of pipelines onto Kubernetes
- Very easy to extend its functionalities through its modular design

# *When should I use it?*
You might consider using *pypz* in the following scenarios:
- you are not a data engineer, but you want to be able to implement and deploy
your data processing logic on your own without caring about data engineering "stuff"
- you are a data engineer, but you are spending too much time setting up the 
conditions to be able to develop data processing pipelines
- you are having data processing workflows (each step executed sequentially)
and you would like to move to data processing pipelines (each step runs parallel)
- you are having a monolithic data processing job, which you would like
to split into more (reusable) operations
- you are struggling scaling out your data processing
- you want to speed up your ETL pipelines
- and more ...

## More concrete use-cases

Your car fleet sends continuously signals to your backend. For each million
signals you want to compute some KPIs as fast as possible to be abla to 
present to your managers.

You are developing some image processing algorithms, which you want to test 
with different parameters on millions of images stored somewhere, and you
want to have as fast iterations as possible.

You are an application engineer, who tries to fine-tune the application
parameters of a SW product with having a customer on your neck.

Your ETL pipeline has to extract images from millions of videos. The images
shall be prepared and stored for further processing.

# Documentation
Check each subproject for the documentation.

# Examples
Starter project template - https://github.com/lazlowa/pypz-starter-template
Usage examples - https://github.com/lazlowa/pypz-examples

---
Special thanks to the colleagues, whose trust and open mindset helped this project
to stay alive: **Neufeld Deniz, Martin Eric, Bauer Tobias Benjamin**
