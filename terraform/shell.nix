with import <bleeding> { };
let
  t = terraform.withPlugins (p: [ p.libvirt p.null p.template p.lxd ]);
in
mkShell {
  buildInputs = [ t tflint ];
}
