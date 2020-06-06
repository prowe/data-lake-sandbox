FROM svajiraya/glue-dev-1.0
SHELL ["/bin/bash", "-c"]

RUN python3 -m pip install pytest pandas

WORKDIR /work
ADD . .

WORKDIR /glue
RUN source ~/.bashrc && /glue/bin/gluepytest /work/glue_scripts/
