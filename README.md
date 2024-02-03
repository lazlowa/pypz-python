# Description

**pypz** (like pipes, pronounce as [paɪps]) is a lightweight and modular Pipeline as Code (PaC) framework.
Its purpose is to simplify and speed up the process of developing **data processing** pipelines by taking 
care most of the challenges and repetitive tasks, so you can focus on the development of the business logic.

Originally, *pypz* has been designed to address some data processing related challenges in
both home and work environment, like:
- how could we speed up and modularize the development of such data processing tasks
- how could we easily perform data processing on huge scale
- how could we speed up the data processing itself

# *When should I consider using it?*
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

<ins>**Realistic use-cases:**</ins>

Your car fleet sends continuously signals to your backend. For each million
signals you want to compute some KPIs as fast as possible to be able to 
present it to your managers.

---

You are developing some image processing algorithms, which you want to test 
with different parameters on millions of images stored somewhere, and you
want to have as fast iterations as possible.

---

You are an application engineer, who tries to fine-tune the application
parameters of a software product with having a customer on your neck.

---

Your ETL pipeline has to extract images from millions of videos. The images
shall be prepared and stored for further processing.

# Links

- [documentation](https://lazlowa.github.io/pypz-python/index.html)
- [starter project template](https://github.com/lazlowa/pypz-starter-template)
- [examples](https://github.com/lazlowa/pypz-examples)

---

Special thanks to the colleagues for being open-minded and for the trust and patience: 
**Deniz Neufeld, Eric Martin, Tobias Benjamin Bauer**
