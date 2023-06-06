def linear_spline(df,
                  x = None,
                  xt = None,
                  knots = None,
                  floor_cap = (-np.inf, np.inf)
                  valid_range = (-np.inf, np.inf)
                 ):
  """ Linear spline function
  param df: Spark df
  type df: Spark df
  param x: variable names to be linear spline transformed
  type x: string
  param xt: variable name prefix for post linear spline transformed
  type xt: string
  param knots: list of numerical numbers used for spline
  type knots: list/tuple
  param floor_cap: floor/cap knots to flatten out
  type floor_cap: list/tuple
  param valid_range: if x is outside valid range, it will be treated as missing
  type valid_range: list/tuple
  
