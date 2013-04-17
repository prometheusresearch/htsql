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
        cut = None
        if isinstance(self.action, SafeProduceAction):
            cut = self.action.cut
        pipe = translate(self.command.syntax, self.action.environment, cut)
        return pipe()


class AnalyzeFetch(Act):

    adapt(FetchCmd, AnalyzeAction)

    def __call__(self):
        pipe = translate(self.command.syntax, self.action.environment)
        return pipe.plan


class ProduceSkip(Act):

    adapt(SkipCmd, ProduceAction)

    def __call__(self):
        profile = decorate_void()
        return Product(profile, None)


class RenderSQL(Act):

    adapt(SQLCmd, RenderAction)

    def __call__(self):
        plan = analyze(self.command.feed)
        status = '200 OK'
        headers = [('Content-Type', 'text/plain; charset=UTF-8')]
        body = []
        if plan.statement:
            queue = [plan.statement]
            while queue:
                statement = queue.pop(0)
                if body:
                    body.append("\n")
                body.append(statement.sql.encode('utf-8'))
                queue.extend(statement.substatements)
        return (status, headers, body)


