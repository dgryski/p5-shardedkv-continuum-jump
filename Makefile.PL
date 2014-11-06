use 5.018002;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'ShardedKV::Continuum::Jump',
    VERSION_FROM      => 'lib/ShardedKV/Continuum/Jump.pm', # finds $VERSION
    PREREQ_PM         => {
        "Algorithm::ConsistentHash::JumpHash" => "0.03",
    }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/ShardedKV/Continuum/Jump.pm', # retrieve abstract from module
       AUTHOR         => 'Damian Gryski <dgryski@>') : ()),
);
