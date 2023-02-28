{{ .LicenseContent }}

=======================================================================
   APACHE HUGEGRAPH (Incubating) SUBCOMPONENTS:

   The Apache HugeGraph(Incubating) project contains subcomponents with separate copyright
   notices and license terms. Your use of the source code for the these
   subcomponents is subject to the terms and conditions of the following
   licenses.
========================================================================

{{ range .Groups }}
========================================================================
{{.LicenseID}} licenses
========================================================================
{{range .Deps}}
    {{.Name}} {{.Version}} {{.LicenseID}}
{{- end }}
{{ end }}
