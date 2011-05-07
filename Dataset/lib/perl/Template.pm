package ReFlow::Dataset::Template;

use strict;

use XML::Twig;
use Data::Dumper;

my $templates;
my $planFileName;

# static method
sub getTemplates {
  my ($planFileNm) = @_;

  $templates = [];
  $planFileName = $planFileNm;
  my $twig= new XML::Twig(TwigRoots => {datasetTemplate => 1},
			  twig_handlers =>
                          { datasetTemplate => \&datasetTemplateElementHandler,
                          },
			  pretty_print => 'indented');
  $twig->parsefile("$ENV{GUS_HOME}/lib/xml/workflowPlans/$planFileNm");

  return $templates;

}

sub datasetTemplateElementHandler {
  my ($twig, $datasetTemplate) = @_;

  my $datasetTemplateAsText = $datasetTemplate->sprint();
  my $template = ReFlow::Dataset::Template->new($planFileName,
						$datasetTemplateAsText,
						$datasetTemplate->{att}->{class});
  push(@$templates, $template);
}

sub new {
  my ($class, $planFileNm, $datasetTemplateText, $datasetClassName) = @_;

  my $self = {};

  bless($self,$class);

  $self->{text} = $datasetTemplateText;
  $self->{class} = $datasetClassName;
  $self->{planFile} = $planFileNm;

  return $self;
}

sub getClassName {
  my ($self) = @_;

  return $self->{class} ;

}

sub getTemplateAsText {
  my ($self) = @_;

    return $self->{text};
}

# add a text instance
sub addInstance {
    my ($self, $dataset) = @_;

    my @templateLines = split(/\n/, $self->getTemplateAsText());
    my @graphLines;

    # keep only the lines in the template that are actual graph
    foreach my $l (@templateLines) {
      push(@graphLines, $l) unless $l =~ /datasetTemplate/ || $l =~ /\<prop/;
    }
    my $graphText = join("\n", @graphLines);
    
    my ($instantiatedGraphText, $err) = 
	ReFlow::Dataset::Classes::substitutePropsIntoXmlText($graphText, $dataset);

    if ($err) {
      die "\nError: <datasetTemplate class=\"$self->{class}\"> in plan file $self->{planFile} has an invalid property macro: $err\n";
    }
    push(@{$self->{instances}}, $graphText);
}

# this is an inefficient implementation, but beats trying to learn
# how to do it efficiently with xml::twig.
sub substituteInstancesIntoPlanText {
    my ($self, $planText) = @_;

    # scan plan, finding our template
    # excise it and replace it with our instances

    my @t = split(/\n/, $planText);

    my @newPlanText;
    my $foundThisTemplate;
    foreach my $line (@t) {
      if ($line =~/\<datasetTemplate/) {
	if ($line =~ /class=\"([^\"]*?)\"/) {
	  my $class = $1;
	  if ($class eq $self->{class}) {
	    $foundThisTemplate = 1;
	  }
	} else {
	  die "\nError: Plan file $self->{planFile} includes a <datasetTemplate> element that does not have class= on the same line\n";
	}
      }
      if ($foundThisTemplate) {
	if ($line =~ /\<\/datasetTemplate/) {
	  $foundThisTemplate = 0;
	  if ($self->{instances}) {
	    push(@newPlanText, join("\n", @{$self->{instances}}));
	  }
	}
      } else {
	push(@newPlanText, $line);
      }
    }

    return join("\n", @newPlanText);
}


1;
