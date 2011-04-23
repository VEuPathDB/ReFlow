package ReFlow::Dataset::Template;

use strict;

use XML::Twig;
use Data::Dumper;

# static method
sub getTemplates {
    my ($planFileName) = @_;

    
}

sub new {
  my ($twig) = @_;

  my $self = {};

  bless($self,$class);

  $self->_parseXmlFile($self->{classesFile});

  return $self;
}

sub getTemplateClass {

}

sub getTemplateAsText {
    if (!$self->{templateAsText}) {
    }
    return $self->{templateAsText};
}

# add a text instance
sub addInstance {
    my ($dataset) = @_;

    my $t = $self->getTemplateAsText();
    
    # substitute props
    foreach my $prop (@{$dataset->getProps()}) {
	
    }
    push(@{$self->{instances}}, $t);
}

sub substituteInstancesIntoPlanText {
    my ($planText) = @_;

}

sub _parseXmlFile {
  my ($self, $classesFile) = @_;

  my $xml = new XML::Simple();
  $self->{data} = eval{ $xml->XMLin($classesFile, SuppressEmpty => undef, KeyAttr => 'resource', ForceArray=>['publication','unpack', 'getAndUnpackOutput', 'resource', 'wdkReference']) };
#  print STDERR Dumper $self->{data};
  die "$@\n$xmlString\nerror processing XML file $resourcesXmlFile\n" if($@);
}

1;
