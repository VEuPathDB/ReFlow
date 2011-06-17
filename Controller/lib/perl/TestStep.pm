package GUS::Workflow::TestStep;

@ISA = (GUS::Workflow::WorkflowStepInvoker);
use strict;
use GUS::Workflow::WorkflowStepInvoker;

sub run {
  my ($self, $test) = @_;

  my $name = $self->getConfig('name');
  my $wait = $self->getConfig('wait');
  my $mood = $self->getSharedConfig('mood');
  my $msg = $self->getParamValue('msg');

  $self->runCmd($test, "echo $name $mood $msg > teststep.out");

  if (!$test) {
    sleep($wait);
  }
}

sub restart {
}

sub undo {

}

sub getConfigDeclaration {
  my $properties =
    [
     # [name, default, description]
     ['name', "", ""],
     ['wait', "", ""],
    ];
  return $properties;
}

sub getParamDeclaration {
  my $properties =
    [
     # [name, default, description]
     ['msg', "", ""],
    ];
  return $properties;
}

sub getDocumentation {
}
