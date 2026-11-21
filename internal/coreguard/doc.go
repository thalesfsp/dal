// Package coreguard holds only tests. They fail if any of dal's core packages
// links Elastic code again, or if linking them starts anything at init. Elastic
// APM is opt-in, through the elasticapm package.
package coreguard
