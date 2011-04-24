package ReFlow::Dataset::Template;

use strict;

use XML::Twig;
use Data::Dumper;

# static method
sub getTemplates {
    my ($planFileName) = @_;


}

sub new {
  my ($class, $twig) = @_;

  my $self = {};

  bless($self,$class);

  $self->_parseXmlFile($self->{classesFile});

  return $self;
}

sub getTemplateClass {

}

sub getTemplateAsText {
  my ($self) = @_;

    if (!$self->{templateAsText}) {
    }
    return $self->{templateAsText};
}

# add a text instance
sub addInstance {
    my ($self, $dataset) = @_;

    my $t = $self->getTemplateAsText();
    
    # substitute props
    foreach my $prop (@{$dataset->getProps()}) {
	
    }
    push(@{$self->{instances}}, $t);
}

sub substituteInstancesIntoPlanText {
    my ($self, $planText) = @_;

}


1;
