#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..adapter import adapt, Utility
from .command import FetchCmd, SkipCmd, SQLCmd
from .act import (analyze, Act, ProduceAction, SafeProduceAction,
                  AnalyzeAction, RenderAction)
from ..domain import Product
from ..tr.translate import translate
from ..tr.decorate import decorate_void


class ProduceFetch(Act):

    adapt(FetchCmd, ProduceAction)

    def __call__(self):
        limit = None
        offset = None
        if isinstance(self.action, SafeProduceAction):
            limit = self.action.cut
            offset = self.action.offset
        batch = self.action.batch
        pipe = translate(self.command.syntax, self.action.environment,
                         limit=limit, offset=offset, batch=batch)
        output = pipe()(None)
        return output


class AnalyzeFetch(Act):

    adapt(FetchCmd, AnalyzeAction)

    def __call__(self):
        pipe = translate(self.command.syntax, self.action.environment)
        return pipe


class ProduceSkip(Act):

    adapt(SkipCmd, ProduceAction)

    def __call__(self):
        profile = decorate_void()
        return Product(profile, None)


class RenderSQL(Act):

    adapt(SQLCmd, RenderAction)

    def __call__(self):
        pipe = analyze(self.command.feed)
        status = '200 OK'
        headers = [('Content-Type', 'text/plain; charset=UTF-8')]
        body = []
        if 'sql' in pipe.properties:
            body.append(pipe.properties['sql'].encode('utf-8'))
        return (status, headers, body)


