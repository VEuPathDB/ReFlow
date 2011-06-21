package ReFlow::TestFlow::TestStep;

@ISA = (ReFlow::Controller::WorkflowStepInvoker);
use strict;
use ReFlow::Controller::WorkflowStepInvoker;

sub new {
  my ($class, $stepName, $workflow) = @_;

  my $self = {
	      workflow=> $workflow,
	      name => $stepName,
	     };
  bless($self,$class);
  return $self;
}

sub run {
  my ($self, $test, $undo) = @_;

  my $name = $self->getConfig('name');
  my $wait = $self->getConfig('wait');
  my $mood = $self->getSharedConfig('mood');
  my $msg = $self->getParamValue('msg');

  $self->runCmd($test, "echo 'testing... $name $mood $msg' > teststep.out");

  if (!$test) {
    sleep($wait);
  }
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
