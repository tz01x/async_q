FROM python:3.8


WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY . /app

EXPOSE 8000
CMD [ "uvicorn", "core.asgi:application", "--reload", "--port", "8000" ]
