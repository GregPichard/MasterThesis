#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  7 12:10:59 2019

@author: gpichard
"""
## Package ''linearmodels'' by Kevin Sheppard
## Excerpt from https://github.com/bashtage/linearmodels/blob/master/linearmodels/panel/model.py
## Retrieved on May 7, 2019

def _prepare_between(self):
    """Prepare values for between estimation of R2"""
    weights = self.weights if self._is_weighted else None
    y = self.dependent.mean('entity', weights=weights).values
    x = self.exog.mean('entity', weights=weights).values
    # Weight transformation
    wcount, wmean = self.weights.count('entity'), self.weights.mean('entity')
    wsum = wcount * wmean
    w = wsum.values
    w = w / w.mean()
    
    return y, x, w

def _rsquared(self, params, reweight=False):
    """Compute alternative measures of R2"""
    if self.has_constant and self.exog.nvar == 1:
        # Constant only fast track
        return 0.0, 0.0, 0.0

    #############################################
    # R2 - Between
    #############################################
    y, x, w = self._prepare_between()
    if np.all(self.weights.values2d == 1.0) and not reweight:
        w = root_w = np.ones_like(w)
    else:
        root_w = np.sqrt(w)
    wx = root_w * x
    wy = root_w * y
    weps = wy - wx @ params
    residual_ss = float(weps.T @ weps)
    e = y
    if self.has_constant:
        e = y - (w * y).sum() / w.sum()

    total_ss = float(w.T @ (e ** 2))
    r2b = 1 - residual_ss / total_ss

    #############################################
    # R2 - Overall
    #############################################
    y = self.dependent.values2d
    x = self.exog.values2d
    w = self.weights.values2d
    root_w = np.sqrt(w)
    wx = root_w * x
    wy = root_w * y
    weps = wy - wx @ params
    residual_ss = float(weps.T @ weps)
    mu = (w * y).sum() / w.sum() if self.has_constant else 0
    we = wy - root_w * mu
    total_ss = float(we.T @ we)
    r2o = 1 - residual_ss / total_ss

    #############################################
    # R2 - Within
    #############################################
    weights = self.weights if self._is_weighted else None
    wy = self.dependent.demean('entity', weights=weights,
                               return_panel=False)
    wx = self.exog.demean('entity', weights=weights, return_panel=False)
    weps = wy - wx @ params
    residual_ss = float(weps.T @ weps)
    total_ss = float(wy.T @ wy)
    if self.dependent.nobs == 1 or (self.exog.nvar == 1 and self.has_constant):
        r2w = 0
    else:
        r2w = 1 - residual_ss / total_ss

    return r2o, r2w, r2b


## Computation of the usual R2

    weps = y - x @ params
## ...
    resid_ss = float(weps.T @ weps)
    if self.has_constant:
        mu = ybar
    else:
        mu = 0
    total_ss = float((y - mu).T @ (y - mu))
    r2 = 1 - resid_ss / total_ss