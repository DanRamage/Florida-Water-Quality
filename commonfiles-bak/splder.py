r"""
Show how to compute a derivative spline.

Scipy's splines are represented in terms of the standard B-spline basis
functions.  In short, a spline of degree ``k`` is represented in terms of the
knots ``t`` and coefficients ``c`` by:

.. math::

    s(x) = \sum_{j=-\infty}^\infty c_{j} B^k_{j}(x)
    \\
    B^0_i(x) = 1, \text{if $t_i \le x < t_{i+1}$, otherwise $0$,}
    \\
    B^k_i(x) = \frac{x - t_i}{t_{i+k} - t_i} B^{k-1}_i(x)
             + \frac{t_{i+k+1} - x}{t_{i+k+1} - t_{i+1}} B^{k-1}_{i+1}(x)

where :math:`c_j` is nonzero only for ``0 <= j <= N`` and the first
and last `k` knots are at the same position and only used to set up
boundary conditions; terms with vanishing denominators are omitted.

One can follow standard spline textbooks here, and work out that by
differentiating this, one obtains another B-spline, of one order
lower:

.. math::

   s'(x) = \sum_{j=-\infty}^\infty d_j B^{k-1}_j(x)
   \\
   d_j = k \frac{c_{j+1} - c_{j}}{t_{j+k+1} - t_{j+1}}

Care needs to be paid at the endpoints: the first knot
should be thrown away since the order is reduced by one.

"""
from __future__ import division

import numpy as np
from scipy.interpolate import splev
from scipy.interpolate import splrep
from scipy.interpolate import UnivariateSpline

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


class MyUnivariateSpline(UnivariateSpline):
    @classmethod
    def _from_tck(cls, t, c, k):
        self = cls.__new__(cls)
        self.ext = 0
        self._eval_args = t, c, k
        #_data == x,y,w,xb,xe,k,s,n,t,c,fp,fpint,nrdata,ier
        self._data = [None,None,None,None,None,k,None,len(t),t,
                      c,None,None,None,None]
        return self

    def derivative_spline(self):
        """
        Compute the derivative spline of a spline in FITPACK tck
        representation.
        """
        t, c, k = self._eval_args
        if k <= 0:
            raise ValueError("Cannot differentiate order 0 spline")
        # Compute the denominator in the differentiation formula.
        dt = t[k+1:-1] - t[1:-k-1]
        # Compute the new coefficients
        d = (c[1:-1-k] - c[:-2-k]) * k / dt
        # Adjust knots
        t2 = t[1:-1]
        # Pad coefficient array to same size as knots (FITPACK convention)
        d = np.r_[d, [0]*k]
        # Done, return a new spline
        return self._from_tck(t2, d, k-1)

from itertools import izip
def pairwise(iterable): # question 5389507
  "s -> (s0,s1), (s2,s3), (s4, s5), ..."
  a = iter(iterable)
  return izip(a, a)
def main():

    x = [.1,.2,.5,.6,.7,.8,.9]
    y = [1,2,3,4,4,5,6]

    x_pair = pairwise(x)
    y_pair = pairwise(y)
    for ((a, b), (c, d)) in zip(x_pair, y_pair):
      print (d - c) / (b - a)

    y = [-1, 5, 6, 4.3, 2, 5.2, 8, 5.6, 1]
    x = range(len(y))
    array_2d = np.array([x,y])
    # get difference in x- and y- direction
    sec_grad_x = np.diff(array_2d,n=1,axis=0)
    sec_grad_y = np.diff(array_2d,n=1,axis=1)
    cp = []
    n = len(y)
    # starts from 1 because diff function gives a forward difference
    for i in range(1,n-1):
        for j in range(1,n-1):
            # check when the difference changes its sign
            if ((sec_grad_x[i-1,j]<0) != (sec_grad_x[i-1+1,j]<0)) and \
               ((sec_grad_y[i,j-1]<0) != (sec_grad_y[i,j-1+1]<0)):
                cp.append([i,j,  y[i,j]])

    cp = np.array(cp)

    # Fit a spline (must use order 4, since roots() works only for order 3)
    s0 = MyUnivariateSpline(x, y, s=0, k=5)

    # Compute the derivative splines of the fitted spline
    s1 = s0.derivative_spline()
    s2 = s1.derivative_spline()

    xs = np.linspace(0, len(y), 16*len(y))
    ys0 = s0(xs)
    ys1 = s1(xs)
    ys2 = s2(xs)

    # Roots (works only with order 3 splines)
    inflection_points = s2.roots()
    print inflection_points
    print "inflection points:  x =", zip(inflection_points, s0(inflection_points))

    plt.ylim(-10, 15)
    plt.plot(x, y, 'o',
            inflection_points, s0(inflection_points), '*',

        	#xs, ys0, '-',
            #xs, ys1, '-',
            #xs, ys2, '-'
    )
    plt.savefig('/users/danramage/tmp/out.png', dpi=96)
    plt.show()

if __name__ == "__main__":
    main()
