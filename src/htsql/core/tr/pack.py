#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..adapter import Adapter, adapt
from ..domain import Record, ID
from .flow import Flow, CollectFlow, SelectionFlow, IdentityFlow
from .pipe import (RecordPipe, AnnihilatePipe, IteratePipe, ComposePipe,
        ValuePipe, SinglePipe)


class PackingState:

    def __init__(self, segment, name):
        self.segment_stack = []
        self.code_pipes_stack = []
        self.dependent_pipes_stack = []
        self.segment = segment
        self.code_pipes = segment.code_pipes[:]
        self.dependent_pipes = segment.dependent_pipes[:]
        self.name = name
        self.name_stack = []
        self.is_top = True

    def push_name(self, name):
        self.name_stack.append(self.name)
        self.name = name

    def pop_name(self):
        self.name = self.name_stack.pop()

    def descend(self, index):
        self.segment_stack.append(self.segment)
        self.code_pipes_stack.append(self.code_pipes)
        self.dependent_pipes_stack.append(self.dependent_pipes)
        self.segment = self.segment.dependents[index]
        self.code_pipes = self.segment.code_pipes[:]
        self.dependent_pipes = self.segment.dependent_pipes[:]

    def ascend(self):
        self.segment = self.segment_stack.pop()
        self.code_pipes = self.code_pipes_stack.pop()
        self.dependent_pipes = self.dependent_pipes_stack.pop()

    def pull_code(self):
        return self.code_pipes.pop(0)

    def pull_dependent(self):
        return self.dependent_pipes.pop(0)

    def pack(self, flow):
        if not isinstance(flow, CollectFlow):
            self.is_top = False
        return Pack.__invoke__(flow, self)


class Pack(Adapter):

    adapt(Flow)

    def __init__(self, flow, state):
        self.flow = flow
        self.state = state

    def __call__(self):
        return self.state.pull_code()


class PackCollect(Pack):

    adapt(CollectFlow)

    def __call__(self):
        if not self.state.is_top:
            dependent_pipe = self.state.pull_dependent()
            self.state.descend(dependent_pipe.index)
            pipe = self.state.pack(self.flow.seed)
            pipe = IteratePipe(pipe)
            pipe = ComposePipe(dependent_pipe, pipe)
            self.state.ascend()
        else:
            self.state.is_top = False
            pipe = self.state.pack(self.flow.seed)
            pipe = IteratePipe(pipe)
        return pipe


class PackSelection(Pack):

    adapt(SelectionFlow)

    def __call__(self):
        test = self.state.pull_code()
        field_pipes = []
        field_names = []
        for field, profile in zip(self.flow.elements,
                                  self.flow.domain.fields):
            field_names.append(profile.tag)
            self.state.push_name(profile.tag)
            field_pipe = self.state.pack(field)
            self.state.pop_name()
            field_pipes.append(field_pipe)
        record_class = Record.make(self.state.name, field_names)
        pipe = RecordPipe(field_pipes, record_class)
        if isinstance(test, ValuePipe) and test.data is True:
            return pipe
        return AnnihilatePipe(test, pipe)


class PackIdentity(Pack):

    adapt(IdentityFlow)

    def __call__(self):
        test = self.state.pull_code()
        field_pipes = []
        for field in self.flow.elements:
            field_pipe = self.state.pack(field)
            field_pipes.append(field_pipe)
        id_class = ID.make(self.flow.domain.dump)
        pipe = RecordPipe(field_pipes, id_class)
        if isinstance(test, ValuePipe) and test.data is True:
            return pipe
        return AnnihilatePipe(test, pipe)


def pack(flow, segment, name):
    state = PackingState(segment, name)
    pipe = state.pack(flow)
    if not isinstance(flow, CollectFlow):
        pipe = IteratePipe(pipe)
        pipe = ComposePipe(pipe, SinglePipe())
    return pipe


